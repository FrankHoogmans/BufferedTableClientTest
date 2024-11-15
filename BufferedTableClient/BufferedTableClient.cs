using System.Diagnostics;
using System.Threading.Channels;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;

public class BufferedTableClientOptions
{
    public int BatchSize { get; set; } = 100;
    public int ChannelCapacity { get; set; } = 10000;
    public int FlushIntervalSeconds { get; set; } = 1;
    public int MaxRetries { get; set; } = 3;
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TableTransactionActionType DefaultTransactionType { get; set; } = TableTransactionActionType.UpsertMerge;
    public bool EnableTelemetry { get; set; } = true;
}

public class BufferedTableClient<T> : IBufferedTableClient<T>
    where T : class, ITableEntity, new()
{
    private readonly TableClient _tableClient;
    private readonly Channel<(T Entity, TableTransactionActionType TransactionType)> _channel;
    private readonly Task _processTask;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ILogger _logger;
    private readonly BufferedTableClientOptions _options;

    public BufferedTableClient(
        TableClient tableClient,
        ILogger logger,
        BufferedTableClientOptions options = null)
    {
        _tableClient = tableClient;
        _logger = logger;
        _options = options ?? new BufferedTableClientOptions();

        var channelOptions = new BoundedChannelOptions(_options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait
        };
        _channel = Channel.CreateBounded<(T, TableTransactionActionType)>(channelOptions);

        _cancellationTokenSource = new CancellationTokenSource();

        // Make sure the task is started before returning
        _processTask = Task.Run(() => ProcessMessagesAsync(_cancellationTokenSource.Token));
    }

    public async Task WriteEntityAsync(
        T entity,
        TableTransactionActionType transactionType = TableTransactionActionType.UpsertMerge)
    {
        await _channel.Writer.WriteAsync((entity, transactionType));
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        var batch = new List<(T Entity, TableTransactionActionType Type)>();
        var lastFlushTime = DateTime.UtcNow;

        try
        {
            // Stop processing when cancellation is requested or the channel is completed
            while (!(cancellationToken.IsCancellationRequested || _channel.Reader.Completion.IsCompleted))
            {
                var shouldFlush = false;

                // Create a timeout token for the read operation
                try
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeoutCts.CancelAfter(TimeSpan.FromSeconds(_options.FlushIntervalSeconds));

                    if (await _channel.Reader.WaitToReadAsync(timeoutCts.Token))
                    {
                        if (_channel.Reader.TryRead(out var item))
                        {
                            batch.Add(item);
                            shouldFlush = batch.Count >= _options.BatchSize;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Timeout occurred, check if we should flush
                    shouldFlush = batch.Any();
                }

                // Check time-based flush
                if (!shouldFlush && (DateTime.UtcNow - lastFlushTime) > TimeSpan.FromSeconds(_options.FlushIntervalSeconds))
                {
                    shouldFlush = batch.Any();
                }

                if (shouldFlush)
                {
                    var batchToFlush = batch.ToList(); // Create a copy of the batch
                    batch.Clear(); // Clear the original batch immediately

                    await FlushBatchAsync(batchToFlush);
                    lastFlushTime = DateTime.UtcNow;

                    if (_options.EnableTelemetry)
                    {
                        _logger.LogInformation(
                            "Flushed batch of {Count} items at {Time}",
                            batchToFlush.Count,
                            DateTime.UtcNow);
                    }
                }
            }

            // Flush of any remaining items
            if (batch.Any())
            {
                await FlushBatchAsync(batch);
            }

            // Process any remaining items in the channel
            while (_channel.Reader.TryRead(out var item))
            {
                batch.Add(item);

                if (batch.Count >= _options.BatchSize)
                {
                    var batchToFlush = batch.ToList();
                    batch.Clear();
                    await FlushBatchAsync(batchToFlush);
                }
            }

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during message processing: {Message}", ex.Message);
            throw;
        }
    }

    private async Task FlushBatchAsync(List<(T Entity, TableTransactionActionType Type)> entities)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var entityGroups = entities
                .GroupBy(e => e.Entity.PartitionKey)
                .ToDictionary(g => g.Key, g => g.ToList());

            var tasks = new List<Task>();

            foreach (var group in entityGroups)
            {
                var partitionEntities = group.Value;

                if (partitionEntities.Count == 1)
                {
                    var (entity, type) = partitionEntities[0];
                    tasks.Add(ProcessSingleEntityWithRetryAsync(entity, type));
                    continue;
                }

                for (int i = 0; i < partitionEntities.Count; i += 100)
                {
                    var batchEntities = partitionEntities.Skip(i).Take(100).ToList();
                    tasks.Add(ProcessBatchWithRetryAsync(batchEntities));
                }
            }

            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing batch to table storage");
            throw;
        }
        finally
        {
            stopwatch.Stop();
            LogBatchMetrics(entities.Count, stopwatch.Elapsed);
        }
    }

    private async Task ProcessSingleEntityWithRetryAsync(
        T entity,
        TableTransactionActionType type)
    {
        var delay = _options.InitialRetryDelay;

        for (int attempt = 0; attempt <= _options.MaxRetries; attempt++)
        {
            try
            {
                // Choose the correct update type based on TableTransactionActionType
                switch (type)
                {
                    case TableTransactionActionType.UpdateMerge:
                    case TableTransactionActionType.UpsertMerge:
                        await _tableClient.UpsertEntityAsync(entity, TableUpdateMode.Merge);
                        break;

                    default:
                        await _tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace);
                        break;
                }
                return;
            }
            catch (Exception ex) when (IsTransientException(ex) && attempt < _options.MaxRetries)
            {
                await Task.Delay(delay);
                delay *= 2; // Exponential backoff
            }
        }

        throw new Exception($"Failed to process entity after {_options.MaxRetries} retries");
    }

    private async Task ProcessBatchWithRetryAsync(
        List<(T Entity, TableTransactionActionType Type)> batchEntities)
    {
        var delay = _options.InitialRetryDelay;

        for (int attempt = 0; attempt <= _options.MaxRetries; attempt++)
        {
            try
            {
                var batch = batchEntities.Select(e =>
                    new TableTransactionAction(e.Type, e.Entity)).ToList();

                await _tableClient.SubmitTransactionAsync(batch);
                return;
            }
            catch (Exception ex) when (IsTransientException(ex) && attempt < _options.MaxRetries)
            {
                await Task.Delay(delay);
                delay *= 2; // Exponential backoff
            }
        }

        throw new Exception($"Failed to process batch after {_options.MaxRetries} retries");
    }

    private bool IsTransientException(Exception ex)
    {
        return ex switch
        {
            RequestFailedException rfe => rfe.Status == 429 || (rfe.Status >= 500 && rfe.Status <= 599),
            TimeoutException => true,
            TaskCanceledException => true,
            _ => false
        };
    }

    private void LogBatchMetrics(int totalEntities, TimeSpan duration)
    {
        if (!_options.EnableTelemetry) return;

        _logger.LogInformation(
            "Batch Processing Metrics: {TotalEntities} entities processed in {DurationMs}ms. " +
            "Channel Capacity: {ChannelCapacity}, Current Usage: {CurrentUsage}",
            totalEntities,
            duration.TotalMilliseconds,
            _options.ChannelCapacity,
            _channel.Reader.Count);
    }
    public async Task FlushAndShutdownAsync()
    {
        try
        {
            // Signal that no more items will be written
            _channel.Writer.Complete();

            // Wait for processing to complete with a timeout
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

            try
            {
                // Wait for the process task to complete
                await Task.WhenAny(_processTask, Task.Delay(TimeSpan.FromSeconds(15)));

                if (!_processTask.IsCompleted)
                {
                    _logger.LogError("Shutdown timed out after 15 seconds");
                    throw new TimeoutException("Shutdown timed out after 15 seconds");
                }

                // Ensure any exceptions are propagated
                await _processTask;
            }
            catch (Exception ex) when (ex is not TimeoutException)
            {
                _logger.LogError(ex, "Error during shutdown");
                throw;
            }
        }
        finally
        {
            await _cancellationTokenSource.CancelAsync();
        }
    }
}
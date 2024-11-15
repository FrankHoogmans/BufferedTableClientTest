using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

class Program
{
    static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                // Add configuration
                services.AddSingleton<IConfiguration>(new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false)
                    .AddEnvironmentVariables()
                    .Build());

                // Add buffered table client
                services.AddBufferedTableClient(options =>
                {
                    options.BatchSize = 50;
                    options.ChannelCapacity = 10000;
                    options.FlushIntervalSeconds = 1;
                    options.MaxRetries = 3;
                    options.EnableTelemetry = true;
                });

                // Add our test runner
                services.AddSingleton<TestRunner>();
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
            })
            .Build();

        // Start the host
        await host.StartAsync();

        // Run the tests
        var runner = host.Services.GetRequiredService<TestRunner>();
        await runner.RunTestsAsync();

        // Stop the host
        Console.WriteLine("Press any key to exit...");
        //Console.ReadKey();
        await host.StopAsync();
    }
}

public class TestEntity : ITableEntity
{
    public string PartitionKey { get; set; }
    public string RowKey { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public Azure.ETag ETag { get; set; }
    public string Data { get; set; }
}

public class TestRunner
{
    private readonly BufferedTableClientFactory _clientFactory;
    private readonly ILogger<TestRunner> _logger;
    private readonly IBufferedTableClient<TestEntity> _client;

    public TestRunner(
        BufferedTableClientFactory clientFactory,
        ILogger<TestRunner> logger)
    {
        _clientFactory = clientFactory;
        _logger = logger;
        _client = _clientFactory.GetClient<TestEntity>("testdata");
    }

    public async Task RunTestsAsync()
    {
        try
        {
            await RunSinglePartitionTest();
            await RunMultiplePartitionTest();
            await RunHighVolumeTest();
            await RunMixedTransactionTypesTest();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during test execution");
        }
    }

    private async Task RunSinglePartitionTest()
    {
        _logger.LogInformation("Running Single Partition Test...");

        for (int i = 0; i < 10; i++)
        {
            var entity = new TestEntity
            {
                PartitionKey = "TestPartition1",
                RowKey = $"Row_{i}",
                Data = $"Test data {i}",
                Timestamp = DateTimeOffset.UtcNow
            };

            await _client.WriteEntityAsync(entity);
        }

        _logger.LogInformation("Single Partition Test Complete");
    }

    private async Task RunMultiplePartitionTest()
    {
        _logger.LogInformation("Running Multiple Partition Test...");

        var partitions = new[] { "A", "B", "C" };
        foreach (var partition in partitions)
        {
            for (int i = 0; i < 5; i++)
            {
                var entity = new TestEntity
                {
                    PartitionKey = partition,
                    RowKey = $"Row_{i}",
                    Data = $"Test data for partition {partition}",
                    Timestamp = DateTimeOffset.UtcNow
                };

                await _client.WriteEntityAsync(entity);
            }
        }

        _logger.LogInformation("Multiple Partition Test Complete");
    }

    private async Task RunHighVolumeTest()
    {
        _logger.LogInformation("Running High Volume Test...");

        var tasks = new List<Task>();
        for (int i = 0; i < 5000; i++)
        {
            var entity = new TestEntity
            {
                PartitionKey = $"Partition_{i % 4}",  // Using 4 different partitions
                RowKey = $"HighVolume_Row_{i}",
                Data = $"High volume test data {i}",
                Timestamp = DateTimeOffset.UtcNow
            };

            await _client.WriteEntityAsync(entity);
        }

        _logger.LogInformation("High Volume Test Complete");
    }

    private async Task RunMixedTransactionTypesTest()
    {
        _logger.LogInformation("Running Mixed Transaction Types Test...");

        // UpsertMerge
        var mergeEntity = new TestEntity
        {
            PartitionKey = "MixedTest",
            RowKey = "Row1",
            Data = "Initial data",
            Timestamp = DateTimeOffset.UtcNow
        };
        await _client.WriteEntityAsync(mergeEntity, TableTransactionActionType.UpsertMerge);

        // UpsertReplace
        var replaceEntity = new TestEntity
        {
            PartitionKey = "MixedTest",
            RowKey = "Row2",
            Data = "Replace data",
            Timestamp = DateTimeOffset.UtcNow
        };
        await _client.WriteEntityAsync(replaceEntity, TableTransactionActionType.UpsertReplace);

        _logger.LogInformation("Mixed Transaction Types Test Complete");
    }
}
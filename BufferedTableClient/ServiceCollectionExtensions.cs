using System.Collections.Concurrent;
using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public interface IBufferedTableClient<T> where T : class, ITableEntity, new()
{
    Task WriteEntityAsync(T entity, TableTransactionActionType transactionType = TableTransactionActionType.UpsertMerge);
    Task FlushAndShutdownAsync();
}

public class BufferedTableClientFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ConcurrentDictionary<string, object> _clients
        = new ConcurrentDictionary<string, object>();

    public BufferedTableClientFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public IBufferedTableClient<T> GetClient<T>(string tableName) where T : class, ITableEntity, new()
    {
        return (IBufferedTableClient<T>)_clients.GetOrAdd(tableName, name =>
        {
            var logger = _serviceProvider.GetRequiredService<ILogger<BufferedTableClient<T>>>();
            var options = _serviceProvider.GetRequiredService<IOptions<BufferedTableClientOptions>>().Value;
            var connectionString = _serviceProvider.GetRequiredService<IConfiguration>()
                .GetConnectionString("TableStorage");

            var tableClient = new TableClient(connectionString, name);
            return new BufferedTableClient<T>(tableClient, logger, options);
        });
    }

    public async Task ShutdownAllClientsAsync()
    {
        foreach (var client in _clients.Values)
        {
            var shutdownMethod = client.GetType().GetMethod("FlushAndShutdownAsync");
            if (shutdownMethod != null)
            {
                await (Task)shutdownMethod.Invoke(client, null);
            }
        }
    }
}

public static class BufferedTableClientServiceExtensions
{
    public static IServiceCollection AddBufferedTableClient(
        this IServiceCollection services,
        Action<BufferedTableClientOptions> configureOptions = null)
    {
        // Register options
        if (configureOptions != null)
        {
            services.Configure(configureOptions);
        }
        else
        {
            services.Configure<BufferedTableClientOptions>(options => { });
        }

        // Register factory
        services.AddSingleton<BufferedTableClientFactory>();

        // Register hosted service for graceful shutdown
        services.AddHostedService<BufferedTableClientShutdownService>();

        return services;
    }
}

public class BufferedTableClientShutdownService : IHostedService
{
    private readonly IHostApplicationLifetime _appLifetime;
    private readonly IServiceProvider _serviceProvider;

    public BufferedTableClientShutdownService(
        IHostApplicationLifetime appLifetime,
        IServiceProvider serviceProvider)
    {
        _appLifetime = appLifetime;
        _serviceProvider = serviceProvider;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _appLifetime.ApplicationStopping.Register(() =>
        {
            // Get all registered factory types of IBufferedTableClient<T>
            var factory = _serviceProvider.GetRequiredService<BufferedTableClientFactory>();
            factory.ShutdownAllClientsAsync().GetAwaiter().GetResult();
        });

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
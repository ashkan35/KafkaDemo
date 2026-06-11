using System.Collections.Concurrent;
using System.Text.Json;
using KafkaDemo.Models;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KafkaDemo.Messaging;

public class RabbitMqPublisher : IAsyncDisposable
{
    private readonly RabbitMqOptions _options;
    private readonly ConnectionFactory _factory;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _queueDeclareLock = new(1, 1);
    private readonly SemaphoreSlim _channelSlots;
    private readonly ConcurrentQueue<IChannel> _channels = new();
    private readonly ConcurrentDictionary<string, bool> _declaredQueues = new();
    private IConnection? _connection;

    public RabbitMqPublisher(IOptions<RabbitMqOptions> options)
    {
        _options = options.Value;
        _factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString)
        };
        _channelSlots = new SemaphoreSlim(Math.Max(1, _options.PublisherChannelPoolSize));
    }

    public async Task PublishUserLoggedInAsync(UserLoggedInEventModel model, CancellationToken cancellationToken)
    {
        await PublishUserLoggedInAsync(model, _options.QueueName, cancellationToken);
    }

    public async Task PublishUserLoggedInBatchAsync(UserLoggedInEventModel model, CancellationToken cancellationToken)
    {
        await PublishUserLoggedInAsync(model, _options.BatchQueueName, cancellationToken);
    }

    private async Task PublishUserLoggedInAsync(
        UserLoggedInEventModel model,
        string queueName,
        CancellationToken cancellationToken)
    {
        var channel = await RentChannelAsync(cancellationToken);
        var returnToPool = true;

        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(model);
            var properties = new BasicProperties
            {
                ContentType = "application/json",
                Persistent = true
            };

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);
        }
        catch
        {
            returnToPool = false;
            throw;
        }
        finally
        {
            await ReturnChannelAsync(channel, returnToPool);
        }
    }

    private async Task<IChannel> RentChannelAsync(CancellationToken cancellationToken)
    {
        await _channelSlots.WaitAsync(cancellationToken);

        try
        {
            // RabbitMQ channels are not thread-safe, so each request gets exclusive use of a pooled channel.
            while (_channels.TryDequeue(out var pooledChannel))
            {
                if (pooledChannel.IsOpen)
                {
                    return pooledChannel;
                }

                await pooledChannel.DisposeAsync();
            }

            var connection = await GetConnectionAsync(cancellationToken);
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            await EnsureQueueDeclaredAsync(channel, _options.QueueName, cancellationToken);
            await EnsureQueueDeclaredAsync(channel, _options.BatchQueueName, cancellationToken);
            return channel;
        }
        catch
        {
            _channelSlots.Release();
            throw;
        }
    }

    private async Task ReturnChannelAsync(IChannel channel, bool returnToPool)
    {
        if (returnToPool && channel.IsOpen)
        {
            _channels.Enqueue(channel);
        }
        else
        {
            await channel.DisposeAsync();
        }

        _channelSlots.Release();
    }

    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            await ClearChannelPoolAsync();

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connection = await _factory.CreateConnectionAsync(cancellationToken);
            _declaredQueues.Clear();
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task EnsureQueueDeclaredAsync(IChannel channel, string queueName, CancellationToken cancellationToken)
    {
        if (_declaredQueues.ContainsKey(queueName))
        {
            return;
        }

        await _queueDeclareLock.WaitAsync(cancellationToken);
        try
        {
            if (_declaredQueues.ContainsKey(queueName))
            {
                return;
            }

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken);

            _declaredQueues[queueName] = true;
        }
        finally
        {
            _queueDeclareLock.Release();
        }
    }

    private async Task ClearChannelPoolAsync()
    {
        while (_channels.TryDequeue(out var channel))
        {
            await channel.DisposeAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ClearChannelPoolAsync();

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        _connectionLock.Dispose();
        _queueDeclareLock.Dispose();
        _channelSlots.Dispose();
    }
}

using System.Text.Json;
using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KafkaDemo.Messaging;

public class RabbitMqUserLoggedInBatchConsumer(
    IServiceScopeFactory serviceScopeFactory,
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqUserLoggedInBatchConsumer> logger) : BackgroundService
{
    private readonly RabbitMqOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString)
        };

        await using var connection = await factory.CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: _options.BatchQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: (ushort)_options.BatchSize,
            global: false,
            cancellationToken: stoppingToken);

        var deliveries = System.Threading.Channels.Channel.CreateUnbounded<BatchDelivery>();

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            try
            {
                var model = JsonSerializer.Deserialize<UserLoggedInEventModel>(eventArgs.Body.Span);

                if (model is null)
                {
                    RabbitBatchBenchmarkCounters.IncrementPersistFailures();
                    logger.LogWarning("Received empty or invalid batch RabbitMQ message.");
                    await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
                    return;
                }

                await deliveries.Writer.WriteAsync(new BatchDelivery(eventArgs.DeliveryTag, model), stoppingToken);
            }
            catch (Exception ex)
            {
                RabbitBatchBenchmarkCounters.IncrementPersistFailures();
                logger.LogError(ex, "Unexpected error while reading batch RabbitMQ message.");
                await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: _options.BatchQueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        await ProcessBatchesAsync(channel, deliveries.Reader, stoppingToken);
    }

    private async Task ProcessBatchesAsync(
        IChannel channel,
        System.Threading.Channels.ChannelReader<BatchDelivery> reader,
        CancellationToken stoppingToken)
    {
        var batch = new List<BatchDelivery>(_options.BatchSize);

        while (await reader.WaitToReadAsync(stoppingToken))
        {
            batch.Clear();

            if (reader.TryRead(out var firstDelivery))
            {
                batch.Add(firstDelivery);
            }

            using var flushTimeout = new CancellationTokenSource(_options.BatchFlushIntervalMs);
            using var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, flushTimeout.Token);

            while (batch.Count < _options.BatchSize)
            {
                try
                {
                    var delivery = await reader.ReadAsync(linkedToken.Token);
                    batch.Add(delivery);
                }
                catch (OperationCanceledException) when (flushTimeout.IsCancellationRequested && !stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }

            await PersistBatchAsync(channel, batch, stoppingToken);
        }
    }

    private async Task PersistBatchAsync(IChannel channel, List<BatchDelivery> batch, CancellationToken stoppingToken)
    {
        if (batch.Count == 0)
        {
            return;
        }

        try
        {
            await using var scope = serviceScopeFactory.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

            var entities = batch.Select(delivery => new UserLoggedInEvent
            {
                UserId = delivery.Model.UserId,
                UserName = delivery.Model.UserName,
                LoggedInAt = delivery.Model.LoggedInAt,
                Description = delivery.Model.Description
            });

            dbContext.UserLoggedInEvents.AddRange(entities);
            await dbContext.SaveChangesAsync(stoppingToken);

            RabbitBatchBenchmarkCounters.AddPersisted(batch.Count);

            foreach (var delivery in batch)
            {
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
            }
        }
        catch (DbUpdateException ex)
        {
            RabbitBatchBenchmarkCounters.AddPersistFailures(batch.Count);
            logger.LogError(ex, "Database error while saving RabbitMQ batch of {BatchCount} user logged-in events.", batch.Count);

            foreach (var delivery in batch)
            {
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, cancellationToken: stoppingToken);
            }
        }
        catch (Exception ex)
        {
            RabbitBatchBenchmarkCounters.AddPersistFailures(batch.Count);
            logger.LogError(ex, "Unexpected error while saving RabbitMQ batch of {BatchCount} user logged-in events.", batch.Count);

            foreach (var delivery in batch)
            {
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
            }
        }
    }

    private sealed record BatchDelivery(ulong DeliveryTag, UserLoggedInEventModel Model);
}

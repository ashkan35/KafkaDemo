using System.Text.Json;
using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KafkaDemo.Messaging;

public class RabbitMqUserLoggedInConsumer(
    IServiceScopeFactory serviceScopeFactory,
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqUserLoggedInConsumer> logger) : BackgroundService
{
    private readonly RabbitMqOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionString)
        };

        await using var connection = await factory.CreateConnectionAsync(stoppingToken);

        var consumerTasks = Enumerable
            .Range(1, Math.Max(1, _options.ConsumerCount))
            .Select(consumerNumber => RunConsumerAsync(connection, consumerNumber, stoppingToken))
            .ToArray();

        await Task.WhenAll(consumerTasks);
    }

    private async Task RunConsumerAsync(IConnection connection, int consumerNumber, CancellationToken stoppingToken)
    {
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: _options.PrefetchCount,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            try
            {
                var model = JsonSerializer.Deserialize<UserLoggedInEventModel>(eventArgs.Body.Span);

                if (model is null)
                {
                    RabbitBenchmarkCounters.IncrementPersistFailures();
                    logger.LogWarning("RabbitMQ consumer {ConsumerNumber} received empty or invalid user logged-in message.", consumerNumber);
                    await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
                    return;
                }

                await using var scope = serviceScopeFactory.CreateAsyncScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                dbContext.UserLoggedInEvents.Add(new UserLoggedInEvent
                {
                    UserId = model.UserId,
                    UserName = model.UserName,
                    LoggedInAt = model.LoggedInAt,
                    Description = model.Description
                });

                await dbContext.SaveChangesAsync(stoppingToken);
                RabbitBenchmarkCounters.IncrementPersisted();
                await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
            }
            catch (DbUpdateException ex)
            {
                RabbitBenchmarkCounters.IncrementPersistFailures();
                logger.LogError(ex, "RabbitMQ consumer {ConsumerNumber} database error while saving user logged-in event.", consumerNumber);
                await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: true, cancellationToken: stoppingToken);
            }
            catch (Exception ex)
            {
                RabbitBenchmarkCounters.IncrementPersistFailures();
                logger.LogError(ex, "RabbitMQ consumer {ConsumerNumber} unexpected error while processing user logged-in event.", consumerNumber);
                await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        logger.LogInformation(
            "RabbitMQ consumer {ConsumerNumber} started for queue {QueueName} with prefetch {PrefetchCount}.",
            consumerNumber,
            _options.QueueName,
            _options.PrefetchCount);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}

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
            prefetchCount: 10000,
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
                    logger.LogWarning("Received empty or invalid user logged-in RabbitMQ message.");
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
                logger.LogError(ex, "Database error while saving RabbitMQ user logged-in event.");
                await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: true, cancellationToken: stoppingToken);
            }
            catch (Exception ex)
            {
                RabbitBenchmarkCounters.IncrementPersistFailures();
                logger.LogError(ex, "Unexpected error while processing RabbitMQ user logged-in event.");
                await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}

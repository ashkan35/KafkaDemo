using Confluent.Kafka;
using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Messaging;
using KafkaDemo.Models;
using KafkaFlow;
using KafkaFlow.Producers;
using KafkaFlow.Serializer;
using Microsoft.EntityFrameworkCore;
using Scalar.AspNetCore;
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("KafkaDemoDbConnection")
        ?? throw new InvalidOperationException("Connection string 'KafkaDemoDbConnection' was not found.")));

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddHostedService<RabbitMqUserLoggedInConsumer>();
builder.Services.AddHostedService<RabbitMqUserLoggedInBatchConsumer>();

builder.Services.AddKafka(configurationBuilder =>
    configurationBuilder
        .UseConsoleLog()
        .AddCluster(cluster =>
        {
            cluster.WithName("DemoCluster");
            cluster.WithBrokers(["localhost:9092", "localhost:9094", "localhost:9096", "localhost:9098", "localhost:9100"]);
 
            cluster.CreateTopicIfNotExists("demo-topic", 3, 3);
            cluster.AddConsumer(consumerConfig =>
            {
                consumerConfig.WithName("DemoConsumer")
                .WithGroupId("demo-consumer-group")
                .Topic("demo-topic")
                .WithAutoCommitIntervalMs(1000)
                .WithBufferSize(1000)
                .WithWorkersCount(3)
                .WithAutoOffsetReset(KafkaFlow.AutoOffsetReset.Earliest)
                .AddMiddlewares(middlewareConfigurationBuilder =>
                {
                    middlewareConfigurationBuilder.AddDeserializer<JsonCoreDeserializer>();
                    middlewareConfigurationBuilder.AddTypedHandlers(handlerConfigurationBuilder =>
                    {
                        handlerConfigurationBuilder.AddHandler<UserLogedInConsumer>();
                    });
                });
                consumerConfig.WithManualMessageCompletion();
                consumerConfig.WithConsumerConfig(new ConsumerConfig
                {
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin
                });
            });

            cluster.AddProducer("demo-producer", producerConfigurationBuilder =>
            {
                producerConfigurationBuilder.DefaultTopic("demo-topic");
                producerConfigurationBuilder.WithAcks(KafkaFlow.Acks.Leader);

                producerConfigurationBuilder.AddMiddlewares(middlewareConfigurationBuilder =>
                {
                    middlewareConfigurationBuilder.AddSerializer<JsonCoreSerializer>();
                });
            });
        }));
// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapOpenApi();
app.MapScalarApiReference();

app.MapPost("/UserLoggedIn", async (
    IProducerAccessor producerAccessor,
    UserLoggedInEventModel model,
    ILogger<Program> logger) =>
{
    try
    {
        var producer = producerAccessor.GetProducer("demo-producer");
        await producer.ProduceAsync(model.UserId, model);
        KafkaBenchmarkCounters.IncrementPublished();
        return Results.Accepted(value: new { Message = "User logged-in event was published to Kafka." });
    }
    catch (Exception ex)
    {
        KafkaBenchmarkCounters.IncrementPublishFailures();
        logger.LogError(ex, "Unexpected error while publishing user logged-in event to Kafka.");

        return Results.Problem(
            title: "Could not publish user logged-in event to Kafka.",
            detail: ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/UserLoggedInDirectSave", async (
    ApplicationDbContext dbContext,
    UserLoggedInEventModel model,
    ILogger<Program> logger) =>
{
    try
    {
        var userLoggedInEvent = new UserLoggedInEvent
        {
            UserId = model.UserId,
            UserName = model.UserName,
            LoggedInAt = model.LoggedInAt,
            Description = model.Description
        };

        dbContext.UserLoggedInEvents.Add(userLoggedInEvent);
        await dbContext.SaveChangesAsync();

        return Results.Ok(new { userLoggedInEvent.Id });
    }
    catch (DbUpdateException ex)
    {
        logger.LogError(ex, "Database error while saving user logged-in event.");

        return Results.Problem(
            title: "Could not save user logged-in event.",
            detail: ex.InnerException?.Message ?? ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Unexpected error while saving user logged-in event.");

        return Results.Problem(
            title: "Could not save user logged-in event.",
            detail: ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/UserLoggedInRabbit", async (
    RabbitMqPublisher publisher,
    UserLoggedInEventModel model,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    try
    {
        await publisher.PublishUserLoggedInAsync(model, cancellationToken);
        RabbitBenchmarkCounters.IncrementPublished();
        return Results.Accepted(value: new { Message = "User logged-in event was published to RabbitMQ." });
    }
    catch (Exception ex)
    {
        RabbitBenchmarkCounters.IncrementPublishFailures();
        logger.LogError(ex, "Unexpected error while publishing user logged-in event to RabbitMQ.");

        return Results.Problem(
            title: "Could not publish user logged-in event.",
            detail: ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapPost("/UserLoggedInRabbitBatch", async (
    RabbitMqPublisher publisher,
    UserLoggedInEventModel model,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    try
    {
        await publisher.PublishUserLoggedInBatchAsync(model, cancellationToken);
        RabbitBatchBenchmarkCounters.IncrementPublished();
        return Results.Accepted(value: new { Message = "User logged-in event was published to RabbitMQ batch queue." });
    }
    catch (Exception ex)
    {
        RabbitBatchBenchmarkCounters.IncrementPublishFailures();
        logger.LogError(ex, "Unexpected error while publishing user logged-in event to RabbitMQ batch queue.");

        return Results.Problem(
            title: "Could not publish user logged-in event to batch queue.",
            detail: ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.MapGet("/rabbit-benchmark-status", () =>
{
    var published = RabbitBenchmarkCounters.Published;
    var persisted = RabbitBenchmarkCounters.Persisted;

    return Results.Ok(new
    {
        published,
        persisted,
        remaining = published - persisted,
        publishFailures = RabbitBenchmarkCounters.PublishFailures,
        persistFailures = RabbitBenchmarkCounters.PersistFailures,
        utcNow = DateTime.UtcNow
    });
});

app.MapPost("/rabbit-benchmark-reset", () =>
{
    RabbitBenchmarkCounters.Reset();
    return Results.Ok(new { Message = "RabbitMQ benchmark counters were reset." });
});

app.MapGet("/rabbit-batch-benchmark-status", () =>
{
    var published = RabbitBatchBenchmarkCounters.Published;
    var persisted = RabbitBatchBenchmarkCounters.Persisted;

    return Results.Ok(new
    {
        published,
        persisted,
        remaining = published - persisted,
        publishFailures = RabbitBatchBenchmarkCounters.PublishFailures,
        persistFailures = RabbitBatchBenchmarkCounters.PersistFailures,
        utcNow = DateTime.UtcNow
    });
});

app.MapPost("/rabbit-batch-benchmark-reset", () =>
{
    RabbitBatchBenchmarkCounters.Reset();
    return Results.Ok(new { Message = "RabbitMQ batch benchmark counters were reset." });
});

app.MapGet("/kafka-benchmark-status", () =>
{
    var published = KafkaBenchmarkCounters.Published;
    var persisted = KafkaBenchmarkCounters.Persisted;

    return Results.Ok(new
    {
        published,
        persisted,
        remaining = published - persisted,
        publishFailures = KafkaBenchmarkCounters.PublishFailures,
        persistFailures = KafkaBenchmarkCounters.PersistFailures,
        utcNow = DateTime.UtcNow
    });
});

app.MapPost("/kafka-benchmark-reset", () =>
{
    KafkaBenchmarkCounters.Reset();
    return Results.Ok(new { Message = "Kafka benchmark counters were reset." });
});
app.UseHttpsRedirection();
var kafkaBus = app.Services.CreateKafkaBus();
await kafkaBus.StartAsync();


app.UseAuthorization();

app.MapControllers();

app.Run();

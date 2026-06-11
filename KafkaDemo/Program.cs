using Confluent.Kafka;
using KafkaDemo.Data;
using KafkaDemo.Entities;
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

builder.Services.AddKafka(configurationBuilder =>
    configurationBuilder
        .UseConsoleLog()
        .AddCluster(cluster =>
        {
            cluster.WithName("DemoCluster");
            cluster.WithBrokers(["localhost:9092", "localhost:9094", "localhost:9096"]);
 
            cluster.CreateTopicIfNotExists("demo-topic", 3, 3);
            cluster.AddConsumer(consumerConfig =>
            {
                consumerConfig.WithName("DemoConsumer")
                .WithGroupId("demo-consumer-group")
                .Topic("demo-topic")
                .WithAutoCommitIntervalMs(1000)
                .WithBufferSize(10)
                .WithWorkersCount(1)
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

app.MapPost("/UserLoggedIn",async(IProducerAccessor producerAccessor,UserLoggedInEventModel model)=>
{
    var producer = producerAccessor.GetProducer("demo-producer");
    await producer.ProduceAsync(model.UserId.ToString(), model);
    return TypedResults.NoContent();
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
app.UseHttpsRedirection();
var kafkaBus = app.Services.CreateKafkaBus();
await kafkaBus.StartAsync();


app.UseAuthorization();

app.MapControllers();

app.Run();

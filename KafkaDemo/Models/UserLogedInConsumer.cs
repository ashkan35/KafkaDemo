using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Messaging;
using KafkaFlow;

namespace KafkaDemo.Models;

public class UserLogedInConsumer(
    ILogger<UserLogedInConsumer> logger,
    IServiceScopeFactory serviceScopeFactory) : IMessageHandler<UserLoggedInEventModel>
{
    public async Task Handle(IMessageContext context, UserLoggedInEventModel message)
    {

        await using var scope = serviceScopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

        dbContext.UserLoggedInEvents.Add(new UserLoggedInEvent
        {
            UserId = message.UserId,
            UserName = message.UserName,
            LoggedInAt = message.LoggedInAt,
            Description = message.Description
        });

        try
        {
            await dbContext.SaveChangesAsync();
            KafkaBenchmarkCounters.IncrementPersisted();
            context.ConsumerContext.Complete();
        }
        catch
        {
            KafkaBenchmarkCounters.IncrementPersistFailures();
            throw;
        }
    }
}

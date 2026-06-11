using KafkaDemo.Data;
using KafkaDemo.Entities;
using KafkaDemo.Messaging;
using KafkaFlow;

namespace KafkaDemo.Models;

public class UserLogedInConsumer(
    ILogger<UserLogedInConsumer> logger,
    ApplicationDbContext dbContext) : IMessageHandler<UserLoggedInEventModel>
{
    public async Task Handle(IMessageContext context, UserLoggedInEventModel message)
    {
        logger.LogWarning("User with {Id} and {UserName} has logged in at {LoggedInAt}", message.UserId, message.UserName, message.LoggedInAt);

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

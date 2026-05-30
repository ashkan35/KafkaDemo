using KafkaFlow;

namespace KafkaDemo.Models;

public class UserLogedInConsumer(ILogger<UserLogedInConsumer> logger) : IMessageHandler<UserLoggedInEventModel>
{
    public Task Handle(IMessageContext context, UserLoggedInEventModel message)
    {
        logger.LogWarning("User with {Id} and {UserName} has logged in at {LoggedInAt}", message.UserId, message.UserName, message.LoggedInAt);
        context.ConsumerContext.Complete();
        return Task.CompletedTask;

    }
}

namespace KafkaDemo.Models;

public record UserLoggedInEventModel(string UserId, string UserName, DateTime LoggedInAt, string Description);

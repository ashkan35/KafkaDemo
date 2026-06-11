namespace KafkaDemo.Models;

public record UserLoggedInEventModel
{
    private const int DefaultDescriptionLength = 10 * 1024;

    public string UserId { get; init; } = string.Empty;

    public string UserName { get; init; } = string.Empty;

    public DateTime LoggedInAt { get; init; }

    public string Description { get; init; } = new('A', DefaultDescriptionLength);
}

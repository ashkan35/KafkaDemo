namespace KafkaDemo.Entities;

public class UserLoggedInEvent
{
    public string Id { get; set; } = Ulid.NewUlid(DateTime.Now).ToString();

    public string UserId { get; set; } = string.Empty;

    public string UserName { get; set; } = string.Empty;

    public DateTime LoggedInAt { get; set; }

    public string Description { get; set; } = string.Empty;
}

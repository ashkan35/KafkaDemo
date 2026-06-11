namespace KafkaDemo.Messaging;

public class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string ConnectionString { get; set; } = string.Empty;

    public string QueueName { get; set; } = "user-logged-in-events";

    public int PublisherChannelPoolSize { get; set; } = 64;
}

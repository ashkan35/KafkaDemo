namespace KafkaDemo.Messaging;

public class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string ConnectionString { get; set; } = string.Empty;

    public string QueueName { get; set; } = "user-logged-in-events";

    public string BatchQueueName { get; set; } = "user-logged-in-events-batch";

    public int PublisherChannelPoolSize { get; set; } = 64;

    public int ConsumerCount { get; set; } = 4;

    public ushort PrefetchCount { get; set; } = 1000;

    public int BatchSize { get; set; } = 1000;

    public int BatchFlushIntervalMs { get; set; } = 1000;
}

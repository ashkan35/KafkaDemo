namespace KafkaDemo.Messaging;

public static class RabbitBatchBenchmarkCounters
{
    private static long _published;
    private static long _persisted;
    private static long _publishFailures;
    private static long _persistFailures;

    public static long Published => Interlocked.Read(ref _published);

    public static long Persisted => Interlocked.Read(ref _persisted);

    public static long PublishFailures => Interlocked.Read(ref _publishFailures);

    public static long PersistFailures => Interlocked.Read(ref _persistFailures);

    public static void IncrementPublished() => Interlocked.Increment(ref _published);

    public static void AddPersisted(long count) => Interlocked.Add(ref _persisted, count);

    public static void IncrementPublishFailures() => Interlocked.Increment(ref _publishFailures);

    public static void IncrementPersistFailures() => Interlocked.Increment(ref _persistFailures);

    public static void AddPersistFailures(long count) => Interlocked.Add(ref _persistFailures, count);

    public static void Reset()
    {
        Interlocked.Exchange(ref _published, 0);
        Interlocked.Exchange(ref _persisted, 0);
        Interlocked.Exchange(ref _publishFailures, 0);
        Interlocked.Exchange(ref _persistFailures, 0);
    }
}

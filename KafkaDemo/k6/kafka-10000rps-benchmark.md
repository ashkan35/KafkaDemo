# Kafka 10000 RPS Benchmark

This benchmark publishes 10 KB `UserLoggedInEventModel` messages to `/UserLoggedIn`.

Kafka setup in `Program.cs`:

- Brokers: `localhost:9092`, `localhost:9094`, `localhost:9096`, `localhost:9098`, `localhost:9100`
- Topic: `demo-topic`
- Producer: `demo-producer`
- Consumer: `DemoConsumer`
- Consumer persistence: `UserLogedInConsumer` saves each consumed message to PostgreSQL

Run from the `KafkaDemo\k6` folder:

```powershell
Invoke-RestMethod -Method Post http://localhost:5192/kafka-benchmark-reset

k6 run user-logged-in-kafka-10000rps.js

$finishTime = Get-Date
Write-Host "k6 finished at: $finishTime"

do {
    $status = Invoke-RestMethod http://localhost:5192/kafka-benchmark-status
    $status | Format-List
    Start-Sleep -Seconds 2
} while ($status.remaining -gt 0)

$drainedAt = Get-Date
Write-Host "Kafka drain completed at: $drainedAt"
Write-Host "Drain time: $($drainedAt - $finishTime)"
```

Run from the repository root:

```powershell
Invoke-RestMethod -Method Post http://localhost:5192/kafka-benchmark-reset

k6 run KafkaDemo\k6\user-logged-in-kafka-10000rps.js

$finishTime = Get-Date
Write-Host "k6 finished at: $finishTime"

do {
    $status = Invoke-RestMethod http://localhost:5192/kafka-benchmark-status
    $status | Format-List
    Start-Sleep -Seconds 2
} while ($status.remaining -gt 0)

$drainedAt = Get-Date
Write-Host "Kafka drain completed at: $drainedAt"
Write-Host "Drain time: $($drainedAt - $finishTime)"
```

Useful overrides:

```powershell
k6 run -e RPS=10000 -e DURATION=2m -e PRE_ALLOCATED_VUS=2000 -e MAX_VUS=10000 KafkaDemo\k6\user-logged-in-kafka-10000rps.js
```

Interpretation:

- `published_events` in k6 is the number of HTTP requests that returned `202`.
- `publish_failures` is the rate of requests that did not return `202`.
- `/kafka-benchmark-status` `persisted` is the number of Kafka messages saved to PostgreSQL.
- `remaining = published - persisted`.
- Drain is complete when `remaining` becomes `0`.
- To validate persisted rows, compare the database row count before and after the test.

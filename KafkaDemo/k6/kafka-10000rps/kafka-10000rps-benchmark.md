# Kafka 10000 RPS Benchmark

This benchmark publishes 10 KB `UserLoggedInEventModel` messages to `/UserLoggedIn`.

The script warms up before the 10000 RPS hold:

- 50 RPS for 10 seconds
- Ramp to 7000 RPS over 20 seconds
- Ramp to 10000 RPS over 10 seconds
- Hold 10000 RPS for 2 minutes

Kafka setup in `Program.cs`:

- Brokers: `localhost:9092`, `localhost:9094`, `localhost:9096`, `localhost:9098`, `localhost:9100`
- Topic: `demo-topic`
- Producer: `demo-producer`
- Consumer: `DemoConsumer`
- Consumer persistence: `UserLogedInConsumer` saves each consumed message to PostgreSQL

Run from the repository root with Performance Monitor collector `Kafka10000`:

```powershell
logman start Kafka10000

try {
    Invoke-RestMethod -Method Post http://localhost:5192/kafka-benchmark-reset

    k6 run KafkaDemo\k6\kafka-10000rps\user-logged-in-kafka-10000rps.js

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
}
finally {
    logman stop Kafka10000
}
```

Run from the `KafkaDemo\k6\kafka-10000rps` folder:

```powershell
logman start DirectSave2500

try {
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
}
finally {
    logman stop DirectSave2500
}
```

Useful overrides:

```powershell
k6 run -e WARMUP_RPS=50 -e WARMUP_DURATION=10s -e BRIDGE_RPS=7000 -e BRIDGE_DURATION=20s -e RPS=10000 -e DURATION=2m -e PRE_ALLOCATED_VUS=2000 -e MAX_VUS=10000 KafkaDemo\k6\kafka-10000rps\user-logged-in-kafka-10000rps.js
```

Interpretation:

- `published_events` is the number of HTTP requests that returned `202`.
- `publish_failures` is the rate of requests that did not return `202`.
- `/kafka-benchmark-status` `persisted` is the number of Kafka messages saved to PostgreSQL.
- `remaining = published - persisted`.
- Drain is complete when `remaining` becomes `0`.
- `http_req_duration` is the client-observed Kafka publish latency.
- `dropped_iterations` means k6 could not maintain the requested arrival rate.
- To validate persisted rows, compare the database row count before and after the test.

If your Performance Monitor collector has a different name, replace `Kafka10000` in the commands. If `logman` says access is denied, run PowerShell as Administrator. If it cannot find the collector, verify the exact name with:

```powershell
logman query
```

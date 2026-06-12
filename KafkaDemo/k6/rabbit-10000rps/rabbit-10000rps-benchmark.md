# RabbitMQ 10000 RPS Benchmark

This benchmark calls `/UserLoggedInRabbit` at 10000 requests per second for 2 minutes.

The payload includes a 10 KB `description` string. The endpoint returns `202` after the message is published to RabbitMQ. Persistence happens asynchronously in the RabbitMQ consumer.

The script warms up before the 10000 RPS hold:

- 50 RPS for 10 seconds
- Ramp to 7000 RPS over 20 seconds
- Ramp to 10000 RPS over 10 seconds
- Hold 10000 RPS for 2 minutes

Run from the repository root with Performance Monitor collector `Rabbit10000`:

```powershell
logman start Rabbit10000

try {
    Invoke-RestMethod -Method Post http://localhost:5192/rabbit-benchmark-reset

    k6 run KafkaDemo\k6\rabbit-10000rps\user-logged-in-rabbit-10000rps.js

    $finishTime = Get-Date
    Write-Host "k6 finished at: $finishTime"

    do {
        $status = Invoke-RestMethod http://localhost:5192/rabbit-benchmark-status
        $status | Format-List
        Start-Sleep -Seconds 2
    } while ($status.remaining -gt 0)

    $drainedAt = Get-Date
    Write-Host "Rabbit drain completed at: $drainedAt"
    Write-Host "Drain time: $($drainedAt - $finishTime)"
}
finally {
    logman stop Rabbit10000
}
```

Run from the `KafkaDemo\k6\rabbit-10000rps` folder:

```powershell
logman start DirectSave2500
try {
    Invoke-RestMethod -Method Post http://localhost:5192/rabbit-benchmark-reset

    k6 run user-logged-in-rabbit-10000rps.js

    $finishTime = Get-Date
    Write-Host "k6 finished at: $finishTime"

    do {
        $status = Invoke-RestMethod http://localhost:5192/rabbit-benchmark-status
        $status | Format-List
        Start-Sleep -Seconds 2
    } while ($status.remaining -gt 0)

    $drainedAt = Get-Date
    Write-Host "Rabbit drain completed at: $drainedAt"
    Write-Host "Drain time: $($drainedAt - $finishTime)"
}
finally {
    logman stop DirectSave2500
}
```

Useful overrides:

```powershell
k6 run -e BASE_URL=http://localhost:5192 -e WARMUP_RPS=50 -e WARMUP_DURATION=10s -e BRIDGE_RPS=7000 -e BRIDGE_DURATION=20s -e RPS=10000 -e DURATION=2m -e PRE_ALLOCATED_VUS=2000 -e MAX_VUS=10000 KafkaDemo\k6\rabbit-10000rps\user-logged-in-rabbit-10000rps.js
```

Interpretation:

- `published_events` is the number of HTTP requests that returned `202`.
- `publish_failures` is the rate of requests that did not return `202`.
- `/rabbit-benchmark-status` `persisted` is the number of RabbitMQ messages saved to PostgreSQL.
- `remaining = published - persisted`.
- Drain is complete when `remaining` becomes `0`.
- `http_req_duration` is the client-observed publish latency.
- `dropped_iterations` means k6 could not maintain the requested arrival rate.

If your Performance Monitor collector has a different name, replace `Rabbit10000` in the commands. If `logman` says access is denied, run PowerShell as Administrator. If it cannot find the collector, verify the exact name with:

```powershell
logman query
```

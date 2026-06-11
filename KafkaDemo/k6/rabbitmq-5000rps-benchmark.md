# RabbitMQ 5000 RPS Benchmark

This runs the RabbitMQ publish endpoint at 5000 requests per second for 2 minutes, then polls the in-memory benchmark counters until all published messages are persisted.

Run from the repository root:

```powershell
logman start DirectSave2500

try {
    Invoke-RestMethod -Method Post http://localhost:5192/rabbit-benchmark-reset

    k6 run user-logged-in-rabbit-5000rps.js

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
k6 run -e RPS=5000 -e DURATION=2m -e PRE_ALLOCATED_VUS=1000 -e MAX_VUS=5000 KafkaDemo\k6\user-logged-in-rabbit-5000rps.js
```

Interpretation:

- `published_events` in k6 is the number of HTTP requests that returned `202`.
- `/rabbit-benchmark-status` `persisted` is the number of Rabbit messages saved to PostgreSQL.
- `remaining = published - persisted`.
- Drain is complete when `remaining` becomes `0`.

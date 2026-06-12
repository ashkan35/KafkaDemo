# RabbitMQ Batch Consumer 15000 RPS Benchmark

This benchmark publishes to `/UserLoggedInRabbitBatch`, which uses the separate RabbitMQ batch queue and batch consumer.

Batch consumer behavior:

- Queue: `user-logged-in-events-batch`
- Prefetch: `1000`
- Database write: `AddRange(...)` followed by one `SaveChangesAsync(...)` per batch
- ACK: only after the batch is saved successfully
- DB failure: NACK with requeue

Run from the `KafkaDemo\k6` folder:

```powershell
Invoke-RestMethod -Method Post http://localhost:5192/rabbit-batch-benchmark-reset

k6 run user-logged-in-rabbit-batch-15000rps.js

$finishTime = Get-Date
Write-Host "k6 finished at: $finishTime"

do {
    $status = Invoke-RestMethod http://localhost:5192/rabbit-batch-benchmark-status
    $status | Format-List
    Start-Sleep -Seconds 2
} while ($status.remaining -gt 0)

$drainedAt = Get-Date
Write-Host "Rabbit batch drain completed at: $drainedAt"
Write-Host "Drain time: $($drainedAt - $finishTime)"
```

Run from the repository root:

```powershell
Invoke-RestMethod -Method Post http://localhost:5192/rabbit-batch-benchmark-reset

k6 run KafkaDemo\k6\user-logged-in-rabbit-batch-15000rps.js

$finishTime = Get-Date
Write-Host "k6 finished at: $finishTime"

do {
    $status = Invoke-RestMethod http://localhost:5192/rabbit-batch-benchmark-status
    $status | Format-List
    Start-Sleep -Seconds 2
} while ($status.remaining -gt 0)

$drainedAt = Get-Date
Write-Host "Rabbit batch drain completed at: $drainedAt"
Write-Host "Drain time: $($drainedAt - $finishTime)"
```

Useful overrides:

```powershell
k6 run -e RPS=15000 -e DURATION=2m -e PRE_ALLOCATED_VUS=3000 -e MAX_VUS=15000 KafkaDemo\k6\user-logged-in-rabbit-batch-15000rps.js
```

Interpretation:

- `published_events` in k6 is the number of HTTP requests that returned `202`.
- `/rabbit-batch-benchmark-status` `persisted` is the number of RabbitMQ messages saved to PostgreSQL.
- `remaining = published - persisted`.
- Drain is complete when `remaining` becomes `0`.

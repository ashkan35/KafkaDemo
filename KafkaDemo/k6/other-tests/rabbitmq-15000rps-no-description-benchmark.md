# RabbitMQ 15000 RPS No-Description Benchmark

This benchmark publishes to `/UserLoggedInRabbit` at 15000 requests per second for 2 minutes.

The k6 payload does not send `description`; the app fills `UserLoggedInEventModel.Description` with its default 10 KB value.

The script defaults to `http://localhost:5192` to avoid local HTTPS overhead during high-RPS tests.
It starts with a 10-second warm-up at 50 RPS, then runs the main 15000 RPS stage.

Run from the `KafkaDemo\k6` folder:

```powershell
Invoke-RestMethod -Method Post http://localhost:5192/rabbit-benchmark-reset

k6 run user-logged-in-rabbit-15000rps-no-description.js

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
```

Run from the repository root:

```powershell
Invoke-RestMethod -Method Post http://localhost:5192/rabbit-benchmark-reset

k6 run KafkaDemo\k6\user-logged-in-rabbit-15000rps-no-description.js

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
```

Useful overrides:

```powershell
k6 run -e WARMUP_RPS=50 -e WARMUP_DURATION=10s -e RPS=15000 -e DURATION=2m -e PRE_ALLOCATED_VUS=3000 -e MAX_VUS=15000 KafkaDemo\k6\user-logged-in-rabbit-15000rps-no-description.js
```

Interpretation:

- `published_events` in k6 is the number of HTTP requests that returned `202`.
- `/rabbit-benchmark-status` `persisted` is the number of RabbitMQ messages saved to PostgreSQL.
- `remaining = published - persisted`.
- Drain is complete when `remaining` becomes `0`.
- Compare DB row count before and after the test if you want an external validation.

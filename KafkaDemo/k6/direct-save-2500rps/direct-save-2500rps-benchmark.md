# Direct Save 2500 RPS Benchmark

This benchmark calls `/UserLoggedInDirectSave` at 2500 requests per second for 2 minutes.

The payload includes a 10 KB `description` string so it matches the earlier direct-save benchmark shape.

Run from the repository root with Performance Monitor collector `DirectSave2500`:

```powershell
logman start DirectSave2500

try {
    k6 run KafkaDemo\k6\direct-save-2500rps\user-logged-in-direct-save-2500rps.js
}
finally {
    logman stop DirectSave2500
}
```

Run from the `KafkaDemo\k6\direct-save-2500rps` folder:

```powershell
logman start DirectSave2500

try {
    k6 run user-logged-in-direct-save-2500rps.js
}
finally {
    logman stop DirectSave2500
}
```

Useful overrides:

```powershell
k6 run -e BASE_URL=http://localhost:5192 -e RPS=2500 -e DURATION=2m -e PRE_ALLOCATED_VUS=500 -e MAX_VUS=10000 KafkaDemo\k6\direct-save-2500rps\user-logged-in-direct-save-2500rps.js
```

Interpretation:

- `saved_events` is the number of requests that returned `200` with an `id`.
- `save_failures` is the rate of requests that did not return a successful save response.
- `http_req_duration` is the client-observed direct-save latency.
- `dropped_iterations` means k6 could not maintain the requested arrival rate.

If `logman` says access is denied, run PowerShell as Administrator. If it cannot find the collector, verify the exact name with:

```powershell
logman query
```

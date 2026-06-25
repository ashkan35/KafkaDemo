# Direct Save 10000 RPS Benchmark

This benchmark calls `/UserLoggedInDirectSave` with a warmup profile:

- 50 RPS for 10 seconds
- Ramp/hold through 7000 RPS for 20 seconds
- Ramp to 10000 RPS over 10 seconds
- Hold 10000 RPS for 2 minutes

The payload includes a 10 KB `description` string.

Run from the repository root with Performance Monitor collector `DirectSave10000`:

```powershell
logman start DirectSave10000

try {
    k6 run KafkaDemo\k6\direct-save-10000rps\user-logged-in-direct-save-10000rps.js
}
finally {
    logman stop DirectSave10000
}
```

Run from the `KafkaDemo\k6\direct-save-10000rps` folder:

```powershell
logman start DirectSave2500

try {
    k6 run user-logged-in-direct-save-10000rps.js
}
finally {
    logman stop DirectSave2500
}
```

Useful overrides:

```powershell
k6 run -e BASE_URL=https://localhost:7189 -e WARMUP_RPS=50 -e WARMUP_DURATION=10s -e BRIDGE_RPS=7000 -e BRIDGE_DURATION=20s -e RPS=10000 -e DURATION=2m -e PRE_ALLOCATED_VUS=2000 -e MAX_VUS=10000 KafkaDemo\k6\direct-save-10000rps\user-logged-in-direct-save-10000rps.js
```

Interpretation:

- `saved_events` is the number of requests that returned `200` with an `id`.
- `save_failures` is the rate of requests that did not return a successful save response.
- `http_req_duration` is the client-observed direct-save latency.
- `dropped_iterations` means k6 could not maintain the requested arrival rate.

If your Performance Monitor collector has a different name, replace `DirectSave10000` in the commands. If `logman` says access is denied, run PowerShell as Administrator. If it cannot find the collector, verify the exact name with:

```powershell
logman query
```

# Direct Save Ramp 5000 To 20000 RPS No-Description Benchmark

This benchmark calls `/UserLoggedInDirectSave` and omits `description` from the JSON payload.

The app fills `UserLoggedInEventModel.Description` with its default 10 KB value.

Stages:

- 5000 RPS for 30 seconds
- 10000 RPS for 30 seconds
- 15000 RPS for 30 seconds
- 20000 RPS for 30 seconds

Run from the `KafkaDemo\k6` folder:

```powershell
k6 run user-logged-in-direct-save-ramp-5000-to-20000rps-no-description.js
```

Run from the repository root:

```powershell
k6 run KafkaDemo\k6\user-logged-in-direct-save-ramp-5000-to-20000rps-no-description.js
```

Useful overrides:

```powershell
k6 run -e PRE_ALLOCATED_VUS=3000 -e MAX_VUS=20000 KafkaDemo\k6\user-logged-in-direct-save-ramp-5000-to-20000rps-no-description.js
```

Interpretation:

- `saved_events` is the number of requests that returned `200` with an `id`.
- `save_failures` is the rate of requests that did not return a successful save response.
- `dropped_iterations` means k6 could not maintain the requested arrival rate.
- The highest healthy stage is the highest RPS where failures stay near zero, dropped iterations stay zero, and latency remains acceptable.

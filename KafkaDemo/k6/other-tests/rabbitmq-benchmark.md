# RabbitMQ Benchmark Notes

Use the RabbitMQ benchmark to measure two separate things:

1. HTTP publish throughput: how many requests per second the API can accept and publish to RabbitMQ.
2. Drain time: how long the consumer needs to persist all published messages to PostgreSQL after k6 stops.

Suggested local flow:

1. Start PostgreSQL, RabbitMQ, and the API.
2. Reset in-memory RabbitMQ benchmark counters:

   ```powershell
   Invoke-RestMethod -Method Post https://localhost:7189/rabbit-benchmark-reset -SkipCertificateCheck
   ```

3. Record the current database row count for `UserLoggedInEvents`.
4. Run k6:

   ```powershell
   k6 run KafkaDemo\k6\user-logged-in-rabbit-1000rps.js
   ```

5. Immediately when k6 finishes, call:

   ```powershell
   Invoke-RestMethod https://localhost:7189/rabbit-benchmark-status -SkipCertificateCheck
   ```

6. Continue polling `/rabbit-benchmark-status` until `remaining` is `0`.
7. Drain time is the timestamp when `remaining` becomes `0` minus the k6 finish time.
8. Validate database inserted rows equals the expected published count.

The status endpoint returns:

```json
{
  "published": 0,
  "persisted": 0,
  "remaining": 0,
  "publishFailures": 0,
  "persistFailures": 0,
  "utcNow": "2026-06-11T00:00:00Z"
}
```

`published` increments only after a successful RabbitMQ publish. `persisted` increments only after `SaveChangesAsync` succeeds and before the message is acknowledged.

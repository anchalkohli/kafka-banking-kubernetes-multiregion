# Safe DLQ Replay Operations

The DLQ replay service performs bounded, audited replay from `dlq-payments-<region>` back to `raw-payments-<region>`.

## Replay request

`POST /replay/{region}` requires the Keycloak `dlq-admin` role and a JSON body:

```json
{
  "partition": 0,
  "startOffset": 1200,
  "endOffsetExclusive": 1250,
  "maxRecords": 50,
  "recordsPerSecond": 100,
  "reason": "Replay after processor defect INC-12345 was fixed",
  "incidentId": "INC-12345"
}
```

Rules:

- region must be `emea`, `nam`, or `aspac`
- one request targets one partition and one explicit offset range
- `endOffsetExclusive - startOffset` must not exceed `maxRecords`
- `maxRecords` is capped at 10,000
- replay rate is capped at 2,000 records/second
- the requested offsets must still be retained in Kafka
- only one replay may run in a region at a time

## Durable audit and checkpointing

Each request creates a PostgreSQL `replay_job` row containing:

- replay job UUID
- region
- partition
- start/end offsets
- next checkpoint offset
- max records and replay rate
- reason and incident ID
- authenticated requester
- status
- replayed record count
- failure text
- creation/update timestamps

The service checkpoints every 100 successfully republished records. A failed job can be resumed from its persisted `next_offset` by the original requester:

`POST /replay/jobs/{jobId}/resume`

Status can be read with:

`GET /replay/jobs/{jobId}`

## Concurrency control

The service uses a PostgreSQL advisory lock keyed by region. Because both Kubernetes replicas share the same regional HA PostgreSQL service, simultaneous EMEA replay requests cannot both execute. NAM and ASPAC can execute independently.

## Rate limiting

Replay is deliberately throttled. This protects the raw topic and processor fleet from a replay storm. The operator chooses `recordsPerSecond` within the configured safety ceiling.

## Database requirement

`dlq-replay-service` requires a regional HA PostgreSQL database. Supply these through the bank-approved secret manager into `banking-secrets`:

- `REPLAY_DB_URL`
- `REPLAY_DB_USER`
- `REPLAY_DB_PASSWORD`

Do not use the single-replica Keycloak PostgreSQL pod as the production replay database.

The base namespace is default-deny. If the HA PostgreSQL endpoint is external to Kubernetes, copy `k8s/security/replay-db-egress-policy.example.yaml` into the regional overlay and replace the documentation CIDR with the smallest approved database CIDR.

## Operating rule

Fix and verify the root cause before replaying. Do not automatically replay a DLQ merely because records exist. Replay should be tied to an incident/change record and observed for re-failure before increasing the replay rate or batch size.

# Safe DLQ Replay Operations

The DLQ replay service performs bounded, audited replay from `dlq-payments-<region>` back to `raw-payments-<region>` using maker-checker approval and a finite Kubernetes Job worker.

## 1. Maker creates a replay request

`POST /replay/{region}` requires `dlq-maker` or `dlq-admin` and creates a `PENDING_APPROVAL` request. It does not execute the replay.

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
- only one replay may execute in a region at a time

## 2. Checker approves

A different authenticated operator approves the request:

`POST /replay/jobs/{jobId}/approve`

This requires `dlq-checker` or `dlq-admin`. The service rejects approval when the approver is the same authenticated identity as the requester.

Approval changes the state from:

```text
PENDING_APPROVAL -> APPROVED
```

and stores `approved_by` and `approved_at` for audit.

## 3. Execute as a finite Kubernetes Job

The long-running API Deployment does not execute approved requests. Use `k8s/jobs/dlq-replay-job.template.yaml` to launch one finite worker for the approved replay UUID.

Before applying the template:

1. replace `REPLACE_WITH_APPROVED_JOB_UUID` with the approved replay job UUID
2. replace `REPLACE_JOB_ID_SHORT` in the Kubernetes Job name with a short unique suffix
3. apply it to the correct regional namespace/cluster

Example:

```bash
kubectl -n banking-emea apply -f /tmp/dlq-replay-job.yaml
```

The worker starts with:

```text
SPRING_MAIN_WEB_APPLICATION_TYPE=none
REPLAY_EXECUTOR_ENABLED=true
REPLAY_JOB_ID=<approved UUID>
```

It validates that the database state is `APPROVED`, obtains the regional PostgreSQL advisory lock, atomically changes the state to `RUNNING`, replays only the bounded offset range, checkpoints progress and exits when the finite job completes.

The Job uses `backoffLimit: 0` intentionally. A failed replay must be inspected and explicitly re-approved/re-run rather than automatically repeating a potentially unsafe operation.

## 4. State model

```text
PENDING_APPROVAL
      |
      | checker approval
      v
APPROVED
      |
      | Kubernetes Job starts
      v
RUNNING
   |      |
   |      +--> FAILED
   |
   +---------> COMPLETED
```

A requester cannot approve their own request. Execution cannot start from `PENDING_APPROVAL` or `FAILED`.

## 5. Durable audit and checkpointing

Each request creates a PostgreSQL `replay_job` row containing:

- replay job UUID
- region
- partition
- start/end offsets
- next checkpoint offset
- max records and replay rate
- reason and incident ID
- authenticated requester
- authenticated approver
- approval timestamp
- status
- replayed record count
- failure text
- creation/update timestamps

The worker checkpoints every 100 successfully republished records.

Status can be read with:

`GET /replay/jobs/{jobId}`

## 6. Concurrency control

The worker uses a PostgreSQL advisory lock keyed by region. Because all API and Job workers use the same regional HA PostgreSQL service, simultaneous EMEA replay executions cannot both run. NAM and ASPAC can execute independently.

## 7. Rate limiting

Replay is deliberately throttled. This protects the raw topic and processor fleet from a replay storm. The maker chooses `recordsPerSecond` within the configured safety ceiling and the checker reviews that requested rate before approval.

## 8. Database requirement

`dlq-replay-service` requires a regional HA PostgreSQL database. Supply these through the bank-approved secret manager into `banking-secrets`:

- `REPLAY_DB_URL`
- `REPLAY_DB_USER`
- `REPLAY_DB_PASSWORD`

Do not use the single-replica Keycloak PostgreSQL pod as the production replay database.

The base namespace is default-deny. If the HA PostgreSQL endpoint is external to Kubernetes, copy `k8s/security/replay-db-egress-policy.example.yaml` into the regional overlay and replace the documentation CIDR with the smallest approved database CIDR.

## 9. Keycloak roles

Create separate operational roles:

```text
dlq-maker    -> create replay request and view status
dlq-checker  -> approve replay request and view status
dlq-admin    -> emergency/admin role, still subject to maker != checker identity check
```

For normal operations, assign maker and checker roles to different operator groups.

## 10. Operating rule

Fix and verify the root cause before requesting replay. Tie each request to an incident/change record. The checker must verify the offset range, reason and replay rate before approval. Observe re-failure and processor lag during execution before approving additional batches.

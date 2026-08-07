# Multi-Region Kubernetes Architecture

## Recommended production topology

Use one Kubernetes cluster per geographic region:

- EMEA cluster -> `banking-emea`
- NAM cluster -> `banking-nam`
- ASPAC cluster -> `banking-aspac`

Each regional cluster runs its own Kafka brokers and banking application workloads. This avoids making application availability dependent on intercontinental Kubernetes control-plane or pod networking.

## Regional flow

```text
IBM MQ (regional)
      |
      v
mq-ingestion-service
      |
      v
raw-payments-<region>
      |
      v
kafka-processor-service
      |
      +--> json-payments-<region>
      |
      +--> dlq-payments-<region>
                  |
                  v
           dlq-replay-service
```

## Kafka topics

EMEA:
- raw-payments-emea: 6 partitions
- json-payments-emea: 6 partitions
- dlq-payments-emea: 2 partitions

NAM:
- raw-payments-nam: 3 partitions
- json-payments-nam: 3 partitions
- dlq-payments-nam: 2 partitions

ASPAC:
- raw-payments-aspac: 3 partitions
- json-payments-aspac: 3 partitions
- dlq-payments-aspac: 2 partitions

## Availability

Application Deployments use multiple replicas, readiness/liveness probes, PodDisruptionBudgets and HorizontalPodAutoscalers.

Kafka runs as a 3-broker KRaft StatefulSet with persistent storage, replication factor 3 and min ISR 2.

## Configuration isolation

Kustomize overlays provide regional values for:
- IBM MQ connection
- IBM MQ queue
- Kafka raw topic
- Kafka output topic
- Kafka DLQ topic
- consumer group
- application region

Credentials remain Kubernetes Secrets and should be replaced by an enterprise secrets solution for production.

## Data residency

The intended topology keeps each region's payment ingestion and Kafka processing inside the corresponding geographic region. Cross-region replication should only be introduced for explicitly approved DR or reporting use cases and should follow banking data-residency requirements.

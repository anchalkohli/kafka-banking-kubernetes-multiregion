# Kafka Banking Kubernetes Multi-Region

Pure Kubernetes deployment model for the Kafka banking architecture across EMEA, NAM, and ASPAC.

## Regions
- EMEA -> namespace `banking-emea`
- NAM -> namespace `banking-nam`
- ASPAC -> namespace `banking-aspac`

## Core flow
IBM MQ -> mq-ingestion-service -> raw-payments-<region> -> kafka-processor-service -> json-payments-<region>

DLQ topic: `dlq-payments-<region>`

## Kubernetes-only
This repository intentionally contains no Docker Compose or systemd deployment model. Runtime deployment is expressed with Kubernetes manifests and Kustomize overlays.

## Layout
- `k8s/base`: reusable application manifests
- `k8s/overlays/emea`: EMEA deployment
- `k8s/overlays/nam`: NAM deployment
- `k8s/overlays/aspac`: ASPAC deployment
- `k8s/platform`: Kafka, Keycloak/Postgres, monitoring and shared platform manifests

## Deploy
```bash
kubectl apply -k k8s/overlays/emea
kubectl apply -k k8s/overlays/nam
kubectl apply -k k8s/overlays/aspac
```

Replace all example credentials and image names before production use.

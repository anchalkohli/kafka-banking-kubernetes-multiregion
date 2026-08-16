# Production transport security

This directory contains the security wiring required for IBM MQ TLS/mTLS and Kafka SSL/SASL_SSL.

## 1. TLS material

Create a regional `banking-tls` Secret from PKCS12 files. Do not commit certificate/private-key material to Git.

```bash
kubectl -n banking-emea create secret generic banking-tls \
  --from-file=mq-truststore.p12=/secure/path/mq-truststore.p12 \
  --from-file=mq-keystore.p12=/secure/path/mq-keystore.p12 \
  --from-file=kafka-truststore.p12=/secure/path/kafka-truststore.p12 \
  --from-file=kafka-keystore.p12=/secure/path/kafka-keystore.p12
```

If IBM MQ uses one-way TLS, omit `mq-keystore.p12`. If Kafka uses server-auth TLS or SASL_SSL without client certificates, omit `kafka-keystore.p12`.

In production, prefer External Secrets Operator, Secrets Store CSI Driver, or the bank-approved secret manager instead of manually creating Secrets.

## 2. Credential Secret

Create `banking-secrets` from the bank-approved secret source. Required keys depend on the selected authentication mechanism:

- `IBM_MQ_USER`
- `IBM_MQ_PASSWORD`
- `IBM_MQ_SSL_TRUSTSTORE_PASSWORD`
- `IBM_MQ_SSL_KEYSTORE_PASSWORD` when MQ mTLS is used
- `KAFKA_SSL_TRUSTSTORE_PASSWORD`
- `KAFKA_SSL_KEYSTORE_PASSWORD` when Kafka mTLS is used
- `KAFKA_SSL_KEY_PASSWORD` when the Kafka private key password differs
- `KAFKA_SASL_JAAS_CONFIG` when SASL is used

Never commit production values to `secret-example.yaml`.

## 3. IBM MQ TLS settings

The regional `banking-config` must set:

```text
IBM_MQ_SSL_ENABLED=true
IBM_MQ_SSL_CIPHER_SUITE=<JSSE CipherSuite matching the MQ SVRCONN CipherSpec>
IBM_MQ_SSL_PEER_NAME=<expected queue-manager certificate DN pattern>
IBM_MQ_SSL_FIPS_REQUIRED=false
IBM_MQ_SSL_TRUSTSTORE_TYPE=PKCS12
IBM_MQ_SSL_KEYSTORE_TYPE=PKCS12
```

The application configures `MQConnectionFactory.setSSLCipherSuite(...)`, optional peer-name validation, and an SSLContext built from the mounted truststore and optional client keystore.

## 4. Kafka transport settings

For TLS only:

```text
KAFKA_SECURITY_PROTOCOL=SSL
```

For SASL over TLS:

```text
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=<bank-approved mechanism, for example SCRAM-SHA-512>
KAFKA_SASL_JAAS_CONFIG=<provided via banking-secrets>
```

All three Java services consume the same Kafka TLS/SASL configuration. The DLQ replay service also passes these properties to its manually created Kafka producer and consumer.

## 5. External IBM MQ NetworkPolicy

The base namespace is default-deny. Therefore an external IBM MQ endpoint remains unreachable until an explicit egress policy is applied.

Copy `mq-egress-policy.example.yaml` into each regional overlay, replace the documentation CIDR with the smallest approved CIDR containing that region's MQ endpoint, and include the file in that overlay's `resources` list.

Do not replace the CIDR with `0.0.0.0/0`.

Example regional intent:

```text
EMEA mq-ingestion -> EMEA MQ CIDR:1414 only
NAM  mq-ingestion -> NAM MQ CIDR:1414 only
ASPAC mq-ingestion -> ASPAC MQ CIDR:1414 only
```

If the cluster CNI supports FQDN-aware egress policies, the bank may choose that instead of IP CIDRs.

## 6. Kafka authorization

Use one Kafka principal per workload/region and grant only the required topic operations. For example, the EMEA ingestion principal should be allowed to produce to `raw-payments-emea` but should not receive broad cluster administration permissions or write to NAM/ASPAC topics.

## 7. Production values still required

The repository intentionally does not invent these bank-specific values:

- MQ endpoint CIDRs
- approved MQ CipherSuite / channel CipherSpec mapping
- queue-manager certificate peer DN
- enterprise CA chain
- client certificates/private keys
- Kafka security protocol and SASL mechanism
- Kafka credentials/principals and ACLs
- secret-manager integration details

Deployment should fail closed until those values are supplied by the target environment.

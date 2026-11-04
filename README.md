# Kafka Real-Time Transaction Streaming

A demo service that shows how to move a batch-style transaction report to an event-driven design using **Java 17, Spring Boot 3 and Apache Kafka**. It is a small reference project inspired by my work on streaming risk and regulatory reporting, built with synthetic data only.

## What it demonstrates

- **Event-driven ingestion:** a REST API publishes transaction events to a Kafka topic instead of writing to a batch job.
- **Ordering per account:** events are keyed by `accountId`, so all events for one account stay ordered inside one partition.
- **Reliable producer:** `acks=all` and an idempotent producer to avoid loss and duplicates on retries.
- **Idempotent consumer:** a replayed or duplicated `transactionId` is ignored, which makes processing safe under at-least-once delivery.
- **Live reporting:** running totals per account are available from `GET /api/transactions/report` as events arrive.
- **Operability:** Spring Boot Actuator health and metrics endpoints.

## Architecture

```
Client --POST--> TransactionController --> Kafka topic "transactions"
                                              |
                                              v
                                   TransactionProcessor (consumer group "reporting")
                                              |
                                              v
                                  GET /api/transactions/report
```

## Tech stack

Java 17, Spring Boot 3.3, Spring for Apache Kafka, Maven, Docker Compose (single-node Kafka in KRaft mode).

## Run it locally

```
docker compose up -d
mvn spring-boot:run
```

Send a transaction:

```
curl -X POST localhost:8080/api/transactions -H "Content-Type: application/json" \
  -d '{"transactionId":"t-1","accountId":"acc-42","amount":125.50,"currency":"USD","occurredAt":"2026-01-01T10:00:00Z"}'
```

Read the live report (send the same `transactionId` twice to see the duplicate counter increase):

```
curl localhost:8080/api/transactions/report
```

## Design notes and trade-offs

- The duplicate-detection set is in memory to keep the demo small. In production this would be a durable store (for example a database unique key or a compacted topic).
- A single-node broker is used for local development only.
- Security (OAuth2/JWT) and secrets management (Azure Key Vault) are intentionally left out of this demo.

## Roadmap

- [ ] Integration tests with Testcontainers
- [ ] Dead-letter topic for poison messages
- [ ] Persist totals in PostgreSQL
- [ ] GitHub Actions CI pipeline
- [ ] Dockerfile and Kubernetes manifests

## Author

Ooha Poloju - Software Engineer (Java, Kafka, Azure, AWS). LinkedIn: https://www.linkedin.com/in/ooha-poloju-3b7b42435/

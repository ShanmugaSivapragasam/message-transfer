# message-transfer (POC)

Spring Boot app (Gradle) that can run locally and later be adapted for **Azure Functions – Flex Consumption**.  
It demonstrates:
- Scheduling (future) orders to a **source** Service Bus queue (BLUE)
- Transferring scheduled orders to a **destination** Service Bus queue (GREEN) by **cancelling & re-scheduling**
- Storing scheduled **sequence numbers in Redis** keyed by `orderId` for future cancellation/transfer
- Validation endpoint to **peek** messages and compare metadata, before/after transfer
- Best-effort error routing to a simple **POC dead-letter** queue

> Note: Real Service Bus DLQ is for *received* messages; scheduling/cancel failures aren’t auto-DLQ’d.  
> For POC we write failed-transfer records to a normal queue `poc-dead-letter`.

## Endpoints

- `POST /api/schedule?count=1000&delaySeconds=3600`  
  Generates and **schedules** fake future orders to the **source** queue.  
  Persists `orderId -> sequenceNumber` in Redis.

- `POST /api/transfer`  
  Reads all `orderId -> sequenceNumber` keys from Redis, **cancels** on source, and **re-schedules** same payload to destination queue.  
  On success, removes the Redis key for that order. On error, writes a small JSON to `poc-dead-letter`.

- `GET /api/validate?peek=10`  
  **Peeks** `peek` messages from **both** queues and prints selected metadata for a quick diff.

## Local Run

1. Start Redis locally (e.g. via Docker):
   ```bash
   docker compose up -d
   ```

2. Copy `src/main/resources/application.properties` and fill Service Bus connection strings & queues.
3. Run:
   ```bash
   ./gradlew bootRun
   ```

## Azure Functions – Flex Consumption (Next Step)

This app is structured so the core services are Spring Beans.  
To move to Functions, create Java HTTP-trigger wrappers that delegate to the same services.  
A sample `functions/` folder with a skeleton is included for reference (not required for local POC).

## Terraform (Optional)

Under `infra/terraform` you’ll find a minimal example for:
- 2 Service Bus namespaces & queues (BLUE/GREEN)
- A basic Azure Cache for Redis (Optional for real; for POC you can use local Redis)

> **Never commit secrets**. Use environment variables or a Key Vault in real setups.

## Data Model

Payload example:
```json
{
  "orderId": "ORD-2025-09-14-000001",
  "placedAt": "2025-09-14T14:30:00Z",
  "lineItems": [
    {"sku":"ITEM-1","name":"Coffee","qty":1,"price":3.50},
    {"sku":"ITEM-2","name":"Fries","qty":1,"price":2.00},
    {"sku":"ITEM-3","name":"Burger","qty":1,"price":6.50}
  ],
  "payment": {"method":"CARD","maskedPan":"**** **** **** 4242","amount":12.00,"currency":"USD"},
  "metadata": {"brand":"generic","channel":"app","version":"1.0.0"}
}
```

Message metadata set:
- `messageId` = `orderId`
- `correlationId` = `orderId`
- `contentType` = `application/json`
- Application properties: `brand`, `channel`, `version`, `scheduledFor`

---

MIT-0 for the POC code.

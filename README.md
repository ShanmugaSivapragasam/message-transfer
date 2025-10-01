# Azure Service Bus Message Transfer

Spring Boot application for transferring scheduled messages between Azure Service Bus queues with Redis state tracking.

## Quick Start

### Prerequisites
- Java 17+
- Azure Service Bus (2 queues: blue → green)
- Redis (local or Azure Cache)

### Setup
```bash
# 1. Clone and setup
git clone https://github.com/ShanmugaSivapragasam/message-transfer.git
cd message-transfer
./setup-local.sh

# 2. Configure credentials
cp env.example .env
# Edit .env with your Azure Service Bus and Redis credentials

# 3. Start Redis (if using local)
brew install redis && brew services start redis

# 4. Run application
./run-app.sh
```

### Test the API
```bash
# Health check
curl http://localhost:8080/api/health

# Schedule 5 orders to source queue (1 hour delay)
curl -X POST "http://localhost:8080/api/schedule?count=5&delaySeconds=3600"

# Transfer all scheduled orders from source → destination (preserves original timing)
curl -X POST http://localhost:8080/api/transfer-enhanced

# Validate transfer timing preservation
curl http://localhost:8080/api/validate/transfer-timings

# Full validation with queue analysis
curl http://localhost:8080/api/validate/full

# Check order status with transfer history
curl http://localhost:8080/api/order/ORD-2025-10-01-000001

# Cancel specific order
curl -X POST http://localhost:8080/api/cancel/ORD-2025-10-01-000001

# Debug Redis state
curl http://localhost:8080/debug/redis/stats
```

## Core Features

- **Message Scheduling**: Schedule orders to source Azure Service Bus queue
- **Safe Transfer**: Cancel from source → reschedule to destination with payload preservation  
- **Redis State Tracking**: Separate key patterns for source/destination sequence numbers
- **Order Lifecycle**: Cancel orders, check status, debug Redis state
- **Error Handling**: Failed transfers logged to error queue

## Redis Data Structure
```
order:source:{orderId}       → source sequence numbers
order:dest:{orderId}         → destination sequence numbers  
order:payload:{orderId}      → original message payloads
order:dest:payload:{orderId} → destination message payloads
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| POST | `/api/schedule` | Schedule orders to source queue |
| POST | `/api/transfer-enhanced` | Transfer orders source → destination **with timing preservation** |
| POST | `/api/cancel/{orderId}` | Cancel specific order |
| GET | `/api/order/{orderId}` | Get order status and transfer history |
| GET | `/api/validate` | Peek both queues for validation |
| GET | `/api/validate/transfer-timings` | Validate transfer timing preservation |
| GET | `/api/validate/full` | Full validation with queue peek + timing analysis |
| GET | `/debug/redis/stats` | Redis state summary |

## Environment Variables

```bash
# Azure Service Bus
AZURE_SERVICEBUS_SOURCE_CONNECTION_STRING="Endpoint=sb://..."
AZURE_SERVICEBUS_DEST_CONNECTION_STRING="Endpoint=sb://..."

# Redis
SPRING_DATA_REDIS_HOST=localhost                    # or Azure Cache host
SPRING_DATA_REDIS_PORT=6379                         # or 6380 for Azure
SPRING_DATA_REDIS_PASSWORD=""                       # empty for local
SPRING_DATA_REDIS_SSL_ENABLED=false                 # true for Azure
```

## Deployment

Ready for Azure Functions Flex Consumption. See `functions/` folder for HTTP trigger examples.

Optional Terraform infrastructure templates in `infra/` folder.

---

**License**: MIT-0

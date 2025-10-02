# Azure Service Bus Message Transfer

Spring Boot application for transferring scheduled messages between Azure Service Bus queues with simplified direct queue inspection approach.

## Architecture Overview

```mermaid
sequenceDiagram
    participant Client
    participant API as REST API
    participant Service as Transfer Service
    participant Redis as Redis Cache
    participant SourceQ as Source Queue<br/>(orders-scheduled-blue)
    participant DestQ as Destination Queue<br/>(orders-scheduled-green)
    participant ErrorQ as Dead Letter Queue<br/>(poc-dead-letter)

    Note over Service: 🎯 Azure Service Bus is Source of Truth<br/>Redis used only for optional debugging

    %% Scheduling Phase
    Client->>+API: POST /api/schedule (orders)
    API->>+Service: scheduleOrders()
    Service->>+SourceQ: scheduleMessage() with future time
    SourceQ-->>-Service: sequenceNumber
    Service->>Redis: Store seq# for debugging (optional)
    Note over Redis: order:source:{orderId}<br/>→ {sequenceNumber, scheduledFor}
    Service-->>-API: [orderId, sequenceNumber, scheduledFor]
    API-->>-Client: Schedule results

    %% Transfer Phase - Direct Queue Inspection
    Client->>+API: POST /api/transfer
    API->>+Service: transferMessagesWithRedisUpdate()
    
    Note over Service: 🔍 DIRECT QUEUE INSPECTION<br/>No Redis dependency
    Service->>+SourceQ: peekMessages(1000) - direct inspection
    SourceQ-->>-Service: All messages in queue
    
    loop For each SCHEDULED message
        Note over Service: ✅ Filter: scheduledEnqueueTime > now()<br/>❌ Skip: active messages
        
        Service->>SourceQ: cancelScheduledMessage(seq)
        Note over Service: 📋 Preserve ALL metadata:<br/>• Original scheduled time<br/>• Message content<br/>• Application properties
        Service->>+DestQ: scheduleMessage() with exact original time
        DestQ-->>-Service: new sequenceNumber
        Service->>Redis: Store destination seq# (optional)
        Note over Redis: order:dest:{orderId}<br/>→ {sequenceNumber, transferredAt}
    end
    
    Service-->>-API: {transferred: N, errors: 0, method: "direct_queue_transfer"}
    API-->>-Client: Transfer results

    %% Error Handling
    Note over Service,ErrorQ: On errors: log to dead letter queue
    Service->>ErrorQ: Send error details
    
    %% Validation
    Client->>+API: GET /api/validate
    API->>Service: validateTransferWithQueuePeek()
    Service->>SourceQ: peekMessages() - check remaining
    Service->>DestQ: peekMessages() - verify transfers
    Service->>Redis: Compare tracking data
    Service-->>API: Validation report
    API-->>-Client: Queue states + timing analysis
```

## Key Architectural Changes

### 🎯 **Direct Queue Inspection (New Approach)**
- **Source of Truth**: Azure Service Bus queues (not Redis)
- **Transfer Logic**: Always peek source queue directly for ALL scheduled messages
- **No Redis Dependency**: Works even if Redis is empty, corrupted, or offline
- **Simplified**: Single code path, no Redis key iteration

### 📊 **Redis Usage (Simplified)**
- **Before**: Required for operation (tracked which messages to transfer)
- **Now**: Optional for debugging only (stores sequence numbers for tracking)
- **Benefits**: More reliable, handles Redis failures gracefully

### 🔄 **Message Processing**
- **Scheduled Messages**: Transfers ALL scheduled messages found in queue
- **Active Messages**: Completely ignored (logged and skipped)
- **Perfect Preservation**: Exact timing, content, and metadata maintained

## Quick Start

### Prerequisites
- Java 17+
- Azure Service Bus (3 queues: source → destination + dead letter)
- Redis (local or Azure Cache) - **Optional for debugging**

### Setup
```bash
# 1. Clone and setup
git clone https://github.com/ShanmugaSivapragasam/message-transfer.git
cd message-transfer
./setup-local.sh

# 2. Configure credentials
cp env.example .env
# Edit .env with your Azure Service Bus and Redis credentials

# 3. Start Redis (optional - only for debugging/tracking)
brew install redis && brew services start redis

# 4. Run application
./run-app.sh
```

### Test the API
```bash
# Health check
curl http://localhost:8080/api/health

# Schedule 5 orders to source queue (scheduled for 5am tomorrow)
curl -X POST "http://localhost:8080/api/schedule?count=5&delaySeconds=54000"

# 🎯 SIMPLIFIED TRANSFER: Direct queue inspection (no Redis dependency)
curl -X POST http://localhost:8080/api/transfer

# Transfer with metadata logging (max 10 samples for testing)
curl -X POST "http://localhost:8080/api/transfer?printMetadata=true"

# Transfer without storing destination tracking in Redis
curl -X POST "http://localhost:8080/api/transfer?cleanupSource=false"

# Debug: Check what messages are actually in queues
curl "http://localhost:8080/api/validate?peek=20"

# Enhanced validation with timing preservation analysis
curl "http://localhost:8080/api/validate?peek=10&includeTimings=true"

# Check transfer status and queue summary
curl http://localhost:8080/api/transfer/status

# Check order status (if Redis tracking available)
curl http://localhost:8080/api/order/ORD-2025-10-01-000001

# Cancel specific order
curl -X POST http://localhost:8080/api/cancel/ORD-2025-10-01-000001

# ⚠️ DANGER: Completely clean up ALL queues (scheduled + active + dead letter)
curl -X DELETE http://localhost:8080/api/cleanup

# Debug Redis state (optional tracking data)
curl http://localhost:8080/debug/redis/stats
```

## Core Features

### 🎯 **Simplified Architecture**
- **Direct Queue Inspection**: Always peek Azure Service Bus source queue directly
- **No Redis Dependency**: Transfer works regardless of Redis state
- **Scheduled Messages Only**: Automatically filters and processes only future-scheduled messages
- ** Preservation**: Maintains exact timing, content, and metadata

### 🔄 **Transfer Logic**
- **Source of Truth**: Azure Service Bus queues (not Redis tracking)
- **Safe Transfer**: Cancel source → preserve all metadata → reschedule to destination
- **Active Message Handling**: Completely ignores active messages (logs and skips)
- **Error Resilience**: Continues processing even if individual messages fail

### 📊 **Optional Redis Tracking**
- **Purpose**: Debugging and sequence number tracking only
- **Not Required**: Transfer works without Redis data
- **Graceful Degradation**: Handles Redis failures transparently

## Redis Data Structure (Optional Debug Info)
```
order:source:{orderId}       → source sequence numbers (scheduling phase)
order:dest:{orderId}         → destination sequence numbers (post-transfer)  
transfer:history:{orderId}   → transfer audit trail
archive:source:{orderId}     → archived source data
```

## API Endpoints

| Method | Endpoint | Description | Redis Required |
|--------|----------|-------------|----------------|
| GET | `/api/health` | Health check | ❌ No |
| POST | `/api/schedule` | Schedule orders to source queue | ❌ No (optional tracking) |
| POST | `/api/transfer` | **Direct queue transfer** - Azure Service Bus source of truth | ❌ No |
| POST | `/api/transfer?printMetadata=true` | Transfer with metadata logging (max 10 samples) | ❌ No |
| POST | `/api/transfer?cleanupSource=false` | Transfer without storing destination tracking | ❌ No |
| POST | `/api/cancel/{orderId}` | Cancel specific order | ⚠️ Needs Redis tracking |
| GET | `/api/order/{orderId}` | Get order status and transfer history | ⚠️ Needs Redis tracking |
| GET | `/api/transfer/status` | Queue summary and transfer status | ❌ No |
| GET | `/api/validate` | Queue validation with direct peek | ❌ No |
| GET | `/api/validate?includeTimings=true` | Enhanced validation with timing analysis | ❌ No |
| DELETE | `/api/cleanup` | **⚠️ DANGER**: Empty ALL queues (scheduled + active + dead letter) | ❌ No |
| GET | `/debug/redis/stats` | Redis state summary | ✅ Yes |

## Environment Variables

```bash
# Azure Service Bus (Required)
AZURE_SERVICEBUS_SOURCE_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=..."
AZURE_SERVICEBUS_DEST_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=..."
AZURE_SERVICEBUS_ERROR_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=..."

# Queue Names
app.source.queue.name=orders-scheduled-blue
app.dest.queue.name=orders-scheduled-green  
app.error.queue.name=poc-dead-letter

# Redis (Optional - for debugging/tracking only)
SPRING_DATA_REDIS_HOST=localhost                    # or Azure Cache host
SPRING_DATA_REDIS_PORT=6379                         # or 6380 for Azure
SPRING_DATA_REDIS_PASSWORD=""                       # empty for local
SPRING_DATA_REDIS_SSL_ENABLED=false                 # true for Azure

# Application Settings
app.defaultScheduleDelaySeconds=3600                # Default delay when scheduling
```

## Transfer Behavior

### ✅ **What Gets Transferred**
- Messages with `scheduledEnqueueTime` > current time
- ALL metadata and application properties preserved
- Exact original scheduled timing maintained
- Complete message payload preserved

### ❌ **What Gets Skipped**
- Active messages (no future scheduled time)
- Messages already delivered/consumed
- Invalid or corrupted messages (logged as errors)

### 🔍 **Transfer Process**
1. **Direct Queue Peek**: Inspect source queue for up to 1000 messages
2. **Filter Scheduled**: Only process messages scheduled for future delivery
3. **Cancel & Transfer**: Cancel source, preserve everything, reschedule to destination
4. **Optional Tracking**: Store destination sequence in Redis for debugging
5. **Error Handling**: Log failures to dead letter queue, continue processing

## Deployment

Ready for Azure Functions Flex Consumption. See `functions/` folder for HTTP trigger examples.

Optional Terraform infrastructure templates in `infra/` folder.

## Troubleshooting

### Transfer Issues
```bash
# Check what messages are actually in source queue
curl "http://localhost:8080/api/validate?peek=50"

# Verify transfer behavior with metadata logging
curl -X POST "http://localhost:8080/api/transfer?printMetadata=true"

# Check application logs for DIRECT_SCHEDULED_FOUND and DIRECT_TRANSFER_SUCCESS
```

### Redis Issues
```bash
# Transfer works without Redis - verify
curl -X POST http://localhost:8080/api/transfer

# Check Redis connection (optional)
curl http://localhost:8080/debug/redis/stats
```

## Benefits of Simplified Architecture

### 🎯 **Reliability Improvements**
- **No Redis Single Point of Failure**: Transfer continues even if Redis is down
- **Source of Truth**: Azure Service Bus queues are authoritative (not Redis tracking)
- **Graceful Degradation**: Missing Redis data doesn't break transfers
- **Error Recovery**: Can recover from Redis data corruption/loss

### ⚡ **Performance Benefits**
- **No Redis Key Iteration**: Eliminates Redis `KEYS` operations
- **Direct Queue Access**: Single peek operation instead of sequence number lookups
- **Reduced Complexity**: Single code path, fewer error conditions
- **Batch Processing**: Handles up to 1000 messages in one operation

### 🛠️ **Operational Advantages**
- **Easier Debugging**: Direct queue inspection shows actual state
- **Simplified Monitoring**: Fewer moving parts to monitor
- **Redis Optional**: Can deploy without Redis for simple use cases
- **Self-Healing**: Automatically discovers all scheduled messages regardless of tracking

### 🔄 **Migration Benefits**
- **Zero Redis Dependency**: Works with empty/corrupted Redis
- **Backward Compatible**: Existing Redis data still used for debugging
- **Progressive Enhancement**: Can add Redis tracking later for advanced features
- **Disaster Recovery**: Easy recovery from Redis failures

---

**License**: MIT-0

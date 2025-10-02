package com.example.messagetransfer.service;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.example.messagetransfer.model.OrderPayload;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class ServiceBusSchedulerService {

    private final ServiceBusSenderClient sourceSender;
    private final ServiceBusSenderClient destSender;
    private final ServiceBusSenderClient errorSender;
    private final ServiceBusReceiverClient sourcePeeker;
    private final ServiceBusReceiverClient destPeeker;
    private final ServiceBusReceiverClient errorReceiver;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${app.defaultScheduleDelaySeconds:3600}")
    private int defaultDelaySeconds;

    @Value("${app.transfer.maxMessages:50000}")
    private int maxTotalMessages;

    public ServiceBusSchedulerService(ServiceBusSenderClient sourceSender,
                                      ServiceBusSenderClient destSender,
                                      ServiceBusSenderClient errorSender,
                                      ServiceBusReceiverClient sourcePeeker,
                                      ServiceBusReceiverClient destPeeker,
                                      ServiceBusReceiverClient errorReceiver,
                                      StringRedisTemplate redis) {
        this.sourceSender = sourceSender;
        this.destSender = destSender;
        this.errorSender = errorSender;
        this.sourcePeeker = sourcePeeker;
        this.destPeeker = destPeeker;
        this.errorReceiver = errorReceiver;
        this.redis = redis;

        // Ensure Java Time (Instant, etc.) serializes correctly
        this.mapper.findAndRegisterModules();
    }

    public List<Map<String, Object>> scheduleOrders(List<OrderPayload> orders, int delaySeconds) {
        int delay = Math.max(delaySeconds, 0);
        List<Map<String, Object>> results = new ArrayList<>(orders.size());

        for (OrderPayload order : orders) {
            try {
                String body = mapper.writeValueAsString(order);
                ServiceBusMessage msg = new ServiceBusMessage(body);
                msg.setMessageId(order.orderId());
                msg.setCorrelationId(order.orderId());
                msg.setContentType("application/json");
                msg.getApplicationProperties().put("brand", order.metadata().brand());
                msg.getApplicationProperties().put("channel", order.metadata().channel());
                msg.getApplicationProperties().put("version", order.metadata().version());

                OffsetDateTime scheduledFor = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(delay);
                msg.getApplicationProperties().put("scheduledFor", scheduledFor.toString());
                Logger.getLogger(ServiceBusSchedulerService.class.getName()).info("Scheduling message: " + body);
                Logger.getLogger(ServiceBusSchedulerService.class.getName()).info("messageId: " + msg.getMessageId() + ", scheduledFor: " + scheduledFor.toString());

                long seq = sourceSender.scheduleMessage(msg, scheduledFor);

                // Store in Redis: Only sequence number and scheduled time (no payload)
                String sourceKey = "order:source:" + order.orderId();
                Map<String, String> orderData = Map.of(
                    "sequenceNumber", Long.toString(seq),
                    "scheduledFor", scheduledFor.toString(),
                    "createdAt", Instant.now().toString()
                );
                
                redis.opsForHash().putAll(sourceKey, orderData);
                redis.expire(sourceKey, 7, TimeUnit.DAYS);

                Logger.getLogger(ServiceBusSchedulerService.class.getName()).info("Stored in Redis: " + sourceKey + " -> seq:" + seq + ", scheduledFor:" + scheduledFor);

                Map<String, Object> r = new LinkedHashMap<>();
                r.put("orderId", order.orderId());
                r.put("sequenceNumber", seq);
                r.put("scheduledFor", scheduledFor.toString());
                results.add(r);
            } catch (Exception ex) {
                results.add(Map.of("orderId", order.orderId(), "error", ex.getMessage()));
            }
        }
        return results;
    }

    public List<Map<String, Object>> validatePeek(int peek) {
        int count = Math.max(1, peek);
        List<Map<String, Object>> src = peekMessages(sourcePeeker, count);
        List<Map<String, Object>> dst = peekMessages(destPeeker, count);
        return List.of(Map.of("source", src), Map.of("destination", dst));
    }

    private List<Map<String, Object>> peekMessages(ServiceBusReceiverClient client, int count) {
        List<Map<String, Object>> out = new ArrayList<>();
        IterableStream<ServiceBusReceivedMessage> stream = client.peekMessages(count);
        for (ServiceBusReceivedMessage m : stream) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("messageId", m.getMessageId());
            row.put("sequenceNumber", m.getSequenceNumber());
            row.put("contentType", m.getContentType());
            row.put("correlationId", m.getCorrelationId());
            row.put("scheduledEnqueueTime", m.getScheduledEnqueueTime()); // OffsetDateTime
            row.put("applicationProperties", new LinkedHashMap<>(m.getApplicationProperties()));
            out.add(row);
        }
        return out;
    }

    /**
     * SIMPLIFIED SCHEDULED MESSAGE TRANSFER:
     * 
     * APPROACH:
     * 1. Always inspect Azure Service Bus queue directly (source of truth)
     * 2. Only handle scheduled messages (ignore active messages completely)
     * 3. Cancel source scheduled message and send to destination with original timing
     * 4. Use Redis only for sequence tracking/debugging (optional)
     * 
     * BENEFITS:
     * - No Redis dependency (Azure Service Bus is source of truth)
     * - Simpler logic (no Redis key iteration)
     * - More reliable (works regardless of Redis state)
     * - Perfect preservation of scheduled messages
     * 
     * @param printMetadata Whether to log detailed metadata for debugging
     * @param cleanupSource Whether to remove source tracking from Redis after transfer (legacy)
     * @return Transfer status with details
     */
    public Map<String, Object> transferMessagesWithRedisUpdate(boolean printMetadata, boolean cleanupSource) {
        Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
            "Starting direct queue transfer - Redis used only for optional sequence tracking/debugging");
        
        return transferScheduledMessagesDirectly(printMetadata, cleanupSource);
    }

    /**
     * Transfer scheduled messages directly from queue without Redis dependency
     * This is now the primary transfer method - Azure Service Bus is the source of truth
     */
    private Map<String, Object> transferScheduledMessagesDirectly(boolean printMetadata, boolean cleanupSource) {
        int transferred = 0;
        int errors = 0;
        int skippedActive = 0;
        List<Map<String, Object>> transferDetails = new ArrayList<>();
        List<Map<String, Object>> metadataLogs = new ArrayList<>();
        int metadataLoggedCount = 0;
        final int MAX_METADATA_LOGS = 10;
        
        // Batching configuration
        final int PEEK_BATCH_SIZE = 250;
        int totalPeeked = 0;
        
        try {
            Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                String.format("Direct queue inspection starting - max messages: %d, batch size: %d", 
                    maxTotalMessages, PEEK_BATCH_SIZE));
            
            // Peek source queue in batches to handle large numbers of messages
            // Azure Service Bus peek limit is ~256 messages, so we'll use 250 per batch
            long fromSequenceNumber = 1; // Start from beginning
            boolean hasMoreMessages = true;
            
            while (hasMoreMessages && totalPeeked < maxTotalMessages) {
                Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                    String.format("Peeking batch starting from sequence %d, batch size %d, total peeked so far: %d", 
                        fromSequenceNumber, PEEK_BATCH_SIZE, totalPeeked));
                
                IterableStream<ServiceBusReceivedMessage> peekedMessages = 
                    sourcePeeker.peekMessages(PEEK_BATCH_SIZE, fromSequenceNumber);
                
                int batchCount = 0;
                long maxSequenceInBatch = fromSequenceNumber;
                
                for (ServiceBusReceivedMessage msg : peekedMessages) {
                    batchCount++;
                    totalPeeked++;
                    maxSequenceInBatch = Math.max(maxSequenceInBatch, msg.getSequenceNumber());
                    
                    try {
                        // Only process scheduled messages (ignore active ones)
                        if (msg.getScheduledEnqueueTime() != null && 
                            msg.getScheduledEnqueueTime().isAfter(OffsetDateTime.now(ZoneOffset.UTC))) {
                        
                        String orderId = msg.getMessageId() != null ? msg.getMessageId() : "unknown-" + msg.getSequenceNumber();
                        
                        Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                            String.format("DIRECT_SCHEDULED_FOUND: orderId=%s, seq=%d, scheduledTime=%s",
                                orderId, msg.getSequenceNumber(), msg.getScheduledEnqueueTime().toString())
                        );
                        
                        // Log source metadata for debugging (max 10)
                        if (printMetadata && metadataLoggedCount < MAX_METADATA_LOGS) {
                            Map<String, Object> sourceMetadata = new LinkedHashMap<>();
                            sourceMetadata.put("stage", "SOURCE_SCHEDULED_DIRECT");
                            sourceMetadata.put("orderId", orderId);
                            sourceMetadata.put("messageId", msg.getMessageId());
                            sourceMetadata.put("correlationId", msg.getCorrelationId());
                            sourceMetadata.put("sequenceNumber", msg.getSequenceNumber());
                            sourceMetadata.put("originalScheduledTime", msg.getScheduledEnqueueTime());
                            sourceMetadata.put("contentType", msg.getContentType());
                            sourceMetadata.put("applicationProperties", new HashMap<>(msg.getApplicationProperties()));
                            sourceMetadata.put("bodySize", msg.getBody() != null ? msg.getBody().toString().length() : 0);
                            sourceMetadata.put("messageType", "SCHEDULED");
                            sourceMetadata.put("method", "DIRECT_QUEUE_INSPECTION");
                            metadataLogs.add(sourceMetadata);
                            metadataLoggedCount++;
                        }
                        
                        // Cancel the source scheduled message
                        sourceSender.cancelScheduledMessage(msg.getSequenceNumber());
                        
                        // Create destination message with exact same content
                        ServiceBusMessage destMsg = new ServiceBusMessage(msg.getBody());
                        destMsg.setMessageId(msg.getMessageId());
                        destMsg.setCorrelationId(msg.getCorrelationId());
                        destMsg.setContentType(msg.getContentType());
                        destMsg.getApplicationProperties().putAll(msg.getApplicationProperties());
                        
                        // Add transfer tracking metadata
                        destMsg.getApplicationProperties().put("transferredFrom", "source");
                        destMsg.getApplicationProperties().put("originalSequence", msg.getSequenceNumber());
                        destMsg.getApplicationProperties().put("transferredAt", Instant.now().toString());
                        destMsg.getApplicationProperties().put("method", "direct_queue_transfer");
                        
                        // Schedule with exact original time
                        long destSeq = destSender.scheduleMessage(destMsg, msg.getScheduledEnqueueTime());
                        
                        // Optional: Store in Redis for debugging/tracking only
                        if (cleanupSource) {
                            // Store destination tracking for debugging
                            String destKey = "order:dest:" + orderId;
                            Map<String, String> destData = Map.of(
                                "sequenceNumber", Long.toString(destSeq),
                                "scheduledFor", msg.getScheduledEnqueueTime().toString(),
                                "transferredAt", Instant.now().toString(),
                                "fromSequence", Long.toString(msg.getSequenceNumber()),
                                "method", "direct_queue_transfer"
                            );
                            redis.opsForHash().putAll(destKey, destData);
                            redis.expire(destKey, 7, TimeUnit.DAYS);
                        }
                        
                        transferred++;
                        transferDetails.add(Map.of(
                            "orderId", orderId,
                            "sourceSequence", msg.getSequenceNumber(),
                            "destSequence", destSeq,
                            "originalScheduledTime", msg.getScheduledEnqueueTime().toString(),
                            "transferredAt", Instant.now().toString(),
                            "timingPreserved", true,
                            "messagePreserved", true,
                            "method", "direct_queue_transfer",
                            "status", "transferred"
                        ));
                        
                        Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                            String.format("DIRECT_TRANSFER_SUCCESS: orderId=%s, sourceSeq=%d, destSeq=%d",
                                orderId, msg.getSequenceNumber(), destSeq)
                        );
                        
                    } else {
                        // Skip active messages (log for debugging)
                        skippedActive++;
                        Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                            String.format("DIRECT_SKIPPING_ACTIVE: messageId=%s, seq=%d, scheduledTime=%s",
                                msg.getMessageId(), msg.getSequenceNumber(),
                                msg.getScheduledEnqueueTime() != null ? msg.getScheduledEnqueueTime().toString() : "NULL")
                        );
                    }
                    
                } catch (Exception ex) {
                    errors++;
                    String orderId = msg.getMessageId() != null ? msg.getMessageId() : "unknown-" + msg.getSequenceNumber();
                    transferDetails.add(Map.of(
                        "orderId", orderId,
                        "sourceSequence", msg.getSequenceNumber(),
                        "error", ex.getMessage(),
                        "status", "failed"
                    ));
                    Logger.getLogger(ServiceBusSchedulerService.class.getName()).warning(
                        String.format("DIRECT_TRANSFER_ERROR: orderId=%s, seq=%d, error=%s", 
                            orderId, msg.getSequenceNumber(), ex.getMessage())
                    );
                }
            }
            
            // Check if we got a full batch (indicating more messages might be available)
            if (batchCount < PEEK_BATCH_SIZE) {
                hasMoreMessages = false;
                Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                    String.format("Reached end of queue - batch size %d < %d, total processed: %d", 
                        batchCount, PEEK_BATCH_SIZE, totalPeeked));
            } else {
                // Move to next batch starting after the highest sequence number we've seen
                fromSequenceNumber = maxSequenceInBatch + 1;
                Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                    String.format("Moving to next batch starting from sequence %d, batch processed: %d", 
                        fromSequenceNumber, batchCount));
            }
        }
        
        Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
            String.format("Completed batched peek operation - total messages examined: %d, transferred: %d", 
                totalPeeked, transferred));
            
        } catch (Exception ex) {
            errors++;
            transferDetails.add(Map.of(
                "error", "Failed to peek source queue: " + ex.getMessage(),
                "status", "failed"
            ));
            Logger.getLogger(ServiceBusSchedulerService.class.getName()).warning(
                "DIRECT_QUEUE_PEEK_ERROR: " + ex.getMessage()
            );
        }
        
        Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
            String.format("DIRECT_TRANSFER_COMPLETE: %d transferred, %d errors, %d skipped_active, %d total_examined. Method=BATCHED_DIRECT_QUEUE_TRANSFER",
                transferred, errors, skippedActive, totalPeeked)
        );
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("transferred", transferred);
        result.put("errors", errors);
        result.put("skippedActive", skippedActive);
        result.put("totalExamined", totalPeeked);
        result.put("details", transferDetails);
        result.put("timestamp", Instant.now().toString());
        result.put("method", "batched_direct_queue_transfer");
        result.put("batchSize", PEEK_BATCH_SIZE);
        result.put("maxTotalMessages", maxTotalMessages);
        result.put("timingPreservationEnabled", true);
        result.put("messagePreservationEnabled", true);
        result.put("messageType", "SCHEDULED_MESSAGES_ONLY");
        result.put("activeMessagesIgnored", true);
        result.put("redisUsage", "optional_debugging_only");
        result.put("sourceOfTruth", "azure_service_bus_queue");
        
        if (printMetadata) {
            result.put("metadataLogs", metadataLogs);
            result.put("metadataLoggingEnabled", true);
            result.put("maxMetadataLogs", MAX_METADATA_LOGS);
            result.put("metadataLoggedCount", metadataLoggedCount);
        }
        
        return result;
    }

    /**
     * Legacy transfer method for backward compatibility
     */
    public Map<String, Object> transferAllFromRedis() {
        return transferMessagesWithRedisUpdate(false, true); // Default: no metadata, cleanup source
    }

    /**
     * Cancel an order by orderId
     * Handles both source and destination cancellations
     */
    public Map<String, Object> cancelOrder(String orderId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("orderId", orderId);
        
        try {
            // Try to cancel from source first
            String sourceKey = "order:source:" + orderId;
            String sourceSeqStr = redis.opsForValue().get(sourceKey);
            
            if (sourceSeqStr != null) {
                long sourceSeq = Long.parseLong(sourceSeqStr);
                try {
                    sourceSender.cancelScheduledMessage(sourceSeq);
                    redis.delete(sourceKey);
                    redis.delete("order:payload:" + orderId);
                    result.put("status", "cancelled_from_source");
                    result.put("sequenceNumber", sourceSeq);
                } catch (Exception cancelEx) {
                    // Message not found - clean up Redis anyway
                    redis.delete(sourceKey);
                    redis.delete("order:payload:" + orderId);
                    result.put("status", "source_not_found_cleaned_redis");
                    result.put("sequenceNumber", sourceSeq);
                    result.put("warning", "Message not found in queue but Redis cleaned up");
                    result.put("error", cancelEx.getMessage());
                }
                return result;
            }
            
            // Try to cancel from destination
            String destKey = "order:dest:" + orderId;
            String destSeqStr = redis.opsForValue().get(destKey);
            
            if (destSeqStr != null) {
                long destSeq = Long.parseLong(destSeqStr);
                try {
                    destSender.cancelScheduledMessage(destSeq);
                    redis.delete(destKey);
                    redis.delete("order:dest:payload:" + orderId);
                    result.put("status", "cancelled_from_destination");
                    result.put("sequenceNumber", destSeq);
                } catch (Exception cancelEx) {
                    // Message not found - clean up Redis anyway
                    redis.delete(destKey);
                    redis.delete("order:dest:payload:" + orderId);
                    result.put("status", "destination_not_found_cleaned_redis");
                    result.put("sequenceNumber", destSeq);
                    result.put("warning", "Message not found in queue but Redis cleaned up");
                    result.put("error", cancelEx.getMessage());
                }
                return result;
            }
            
            result.put("status", "not_found");
            result.put("message", "Order not found in Redis tracking");
            
        } catch (Exception ex) {
            result.put("status", "error");
            result.put("error", ex.getMessage());
        }
        
        return result;
    }

    /**
     * Get order information from Redis including transfer history
     */
    public Map<String, Object> getOrderInfo(String orderId) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("orderId", orderId);
        
        // Check source
        String sourceKey = "order:source:" + orderId;
        String sourceSeq = redis.opsForValue().get(sourceKey);
        if (sourceSeq != null) {
            info.put("sourceSequence", Long.parseLong(sourceSeq));
            info.put("location", "source");
        }
        
        // Check destination
        String destKey = "order:dest:" + orderId;
        String destSeq = redis.opsForValue().get(destKey);
        if (destSeq != null) {
            info.put("destSequence", Long.parseLong(destSeq));
            info.put("location", "destination");
        }
        
        // Check payload availability
        String payloadKey = "order:payload:" + orderId;
        String destPayloadKey = "order:dest:payload:" + orderId;
        boolean hasPayload = redis.hasKey(payloadKey) || redis.hasKey(destPayloadKey);
        info.put("hasPayload", hasPayload);
        
        // Check transfer history
        String transferKey = "transfer:history:" + orderId;
        String transferHistory = redis.opsForValue().get(transferKey);
        if (transferHistory != null) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> history = mapper.readValue(transferHistory, Map.class);
                info.put("transferHistory", history);
            } catch (Exception ex) {
                info.put("transferHistoryError", ex.getMessage());
            }
        }
        
        if (sourceSeq == null && destSeq == null) {
            info.put("status", "not_found");
        } else {
            info.put("status", "tracked");
        }
        
        return info;
    }

    /**
     * Validate and compare timings between source and destination queues
     */
    public Map<String, Object> validateTransferTimings() {
        Map<String, Object> validation = new LinkedHashMap<>();
        List<Map<String, Object>> timingComparisons = new ArrayList<>();
        
        Set<String> transferKeys = redis.keys("transfer:history:*");
        if (transferKeys == null || transferKeys.isEmpty()) {
            validation.put("message", "No transfer history found");
            validation.put("comparisons", timingComparisons);
            return validation;
        }
        
        for (String transferKey : transferKeys) {
            try {
                String orderId = transferKey.substring("transfer:history:".length());
                String historyJson = redis.opsForValue().get(transferKey);
                
                if (historyJson != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> history = mapper.readValue(historyJson, Map.class);
                    
                    Map<String, Object> comparison = new LinkedHashMap<>();
                    comparison.put("orderId", orderId);
                    comparison.put("sourceSequence", history.get("sourceSequence"));
                    comparison.put("destSequence", history.get("destSequence"));
                    comparison.put("originalScheduledTime", history.get("originalScheduledTime"));
                    comparison.put("transferredAt", history.get("transferredAt"));
                    comparison.put("fromQueue", history.get("fromQueue"));
                    comparison.put("toQueue", history.get("toQueue"));
                    
                    // Validate timing preservation
                    String originalTime = (String) history.get("originalScheduledTime");
                    try {
                        OffsetDateTime original = OffsetDateTime.parse(originalTime);
                        OffsetDateTime transferred = OffsetDateTime.parse((String) history.get("transferredAt"));
                        long preservationDelayMinutes = java.time.Duration.between(transferred, original).toMinutes();
                        comparison.put("timingPreservationDelayMinutes", preservationDelayMinutes);
                        comparison.put("timingPreserved", true);
                    } catch (Exception ex) {
                        comparison.put("timingValidationError", ex.getMessage());
                        comparison.put("timingPreserved", false);
                    }
                    
                    timingComparisons.add(comparison);
                }
            } catch (Exception ex) {
                timingComparisons.add(Map.of(
                    "orderId", transferKey.substring("transfer:history:".length()),
                    "error", ex.getMessage()
                ));
            }
        }
        
        validation.put("totalTransfers", timingComparisons.size());
        validation.put("comparisons", timingComparisons);
        validation.put("validatedAt", Instant.now().toString());
        
        return validation;
    }

    /**
     * Enhanced validation that peeks both queues and compares with transfer history
     */
    public Map<String, Object> validateTransferWithQueuePeek(int peekCount) {
        Map<String, Object> validation = new LinkedHashMap<>();
        
        // Get current queue states
        List<Map<String, Object>> sourceMessages = peekMessages(sourcePeeker, peekCount);
        List<Map<String, Object>> destMessages = peekMessages(destPeeker, peekCount);
        
        // Get transfer history
        Map<String, Object> timingValidation = validateTransferTimings();
        
        // Cross-reference queue contents with Redis tracking
        Set<String> sourceKeys = redis.keys("order:source:*");
        Set<String> destKeys = redis.keys("order:dest:*");
        
        List<String> sourceOrderIds = sourceKeys != null ? 
            sourceKeys.stream().map(k -> k.substring("order:source:".length())).toList() : 
            List.of();
            
        List<String> destOrderIds = destKeys != null ? 
            destKeys.stream().map(k -> k.substring("order:dest:".length())).toList() : 
            List.of();
        
        validation.put("queueStates", Map.of(
            "source", Map.of(
                "messagesInQueue", sourceMessages.size(),
                "trackedInRedis", sourceOrderIds.size(),
                "messages", sourceMessages
            ),
            "destination", Map.of(
                "messagesInQueue", destMessages.size(),
                "trackedInRedis", destOrderIds.size(),
                "messages", destMessages
            )
        ));
        
        validation.put("timingValidation", timingValidation);
        validation.put("redisTracking", Map.of(
            "sourceOrders", sourceOrderIds,
            "destOrders", destOrderIds
        ));
        validation.put("validatedAt", Instant.now().toString());
        
        return validation;
    }

    /**
     * COMPREHENSIVE CLEANUP: Delete ALL messages from ALL queues
     * 
     * This method:
     * 1. Cancels ALL scheduled messages by peeking and cancelling
     * 2. Receives and deletes ALL active messages 
     * 3. Purges dead letter queue
     * 4. Clears ALL Redis tracking data
     * 
     * DANGER: This will completely empty all queues!
     * Use only for testing/development!
     */
    public Map<String, Object> cleanupAllQueues() {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> operations = new ArrayList<>();
        
        int totalScheduledCancelled = 0;
        int totalRedisCleared = 0;
        int totalErrors = 0;
        
        try {
            operations.add("=== COMPREHENSIVE QUEUE CLEANUP STARTED ===");
            
            // STEP 1: Cancel ALL scheduled messages in SOURCE queue
            operations.add("STEP 1: Cancelling scheduled messages in SOURCE queue...");
            try {
                IterableStream<ServiceBusReceivedMessage> sourceScheduled = sourcePeeker.peekMessages(1000);
                for (ServiceBusReceivedMessage msg : sourceScheduled) {
                    try {
                        sourceSender.cancelScheduledMessage(msg.getSequenceNumber());
                        totalScheduledCancelled++;
                        operations.add("✓ Cancelled SOURCE scheduled: seq=" + msg.getSequenceNumber() + ", msgId=" + msg.getMessageId());
                    } catch (Exception ex) {
                        totalErrors++;
                        operations.add("✗ Failed to cancel SOURCE scheduled: seq=" + msg.getSequenceNumber() + ", error=" + ex.getMessage());
                    }
                }
            } catch (Exception ex) {
                operations.add("✗ Error peeking SOURCE queue: " + ex.getMessage());
                totalErrors++;
            }
            
            // STEP 2: Cancel ALL scheduled messages in DESTINATION queue  
            operations.add("STEP 2: Cancelling scheduled messages in DESTINATION queue...");
            try {
                IterableStream<ServiceBusReceivedMessage> destScheduled = destPeeker.peekMessages(1000);
                for (ServiceBusReceivedMessage msg : destScheduled) {
                    try {
                        destSender.cancelScheduledMessage(msg.getSequenceNumber());
                        totalScheduledCancelled++;
                        operations.add("✓ Cancelled DEST scheduled: seq=" + msg.getSequenceNumber() + ", msgId=" + msg.getMessageId());
                    } catch (Exception ex) {
                        totalErrors++;
                        operations.add("✗ Failed to cancel DEST scheduled: seq=" + msg.getSequenceNumber() + ", error=" + ex.getMessage());
                    }
                }
            } catch (Exception ex) {
                operations.add("✗ Error peeking DEST queue: " + ex.getMessage());
                totalErrors++;
            }
            
            // STEP 3: Consume and delete ALL active messages from SOURCE queue
            operations.add("STEP 3: Consuming active messages from SOURCE queue...");
            try {
                boolean hasMoreSource = true;
                int sourceActiveCount = 0;
                while (hasMoreSource && sourceActiveCount < 1000) { // Safety limit
                    IterableStream<ServiceBusReceivedMessage> activeMessages = sourcePeeker.receiveMessages(32, java.time.Duration.ofSeconds(3));
                    int batchCount = 0;
                    for (ServiceBusReceivedMessage msg : activeMessages) {
                        try {
                            sourcePeeker.complete(msg); // Complete to delete the message
                            sourceActiveCount++;
                            batchCount++;
                            operations.add("✓ Consumed SOURCE active: seq=" + msg.getSequenceNumber() + ", msgId=" + msg.getMessageId());
                        } catch (Exception ex) {
                            totalErrors++;
                            operations.add("✗ Failed to consume SOURCE active: seq=" + msg.getSequenceNumber() + ", error=" + ex.getMessage());
                        }
                    }
                    hasMoreSource = batchCount > 0;
                    if (!hasMoreSource) {
                        operations.add("SOURCE active messages consumed: " + sourceActiveCount + " total");
                    }
                }
                totalScheduledCancelled += sourceActiveCount; // Add to total operations
            } catch (Exception ex) {
                operations.add("✗ Error consuming SOURCE active messages: " + ex.getMessage());
                totalErrors++;
            }
            
            // STEP 4: Consume and delete ALL active messages from DESTINATION queue
            operations.add("STEP 4: Consuming active messages from DESTINATION queue...");
            try {
                boolean hasMoreDest = true;
                int destActiveCount = 0;
                while (hasMoreDest && destActiveCount < 1000) { // Safety limit
                    IterableStream<ServiceBusReceivedMessage> activeMessages = destPeeker.receiveMessages(32, java.time.Duration.ofSeconds(3));
                    int batchCount = 0;
                    for (ServiceBusReceivedMessage msg : activeMessages) {
                        try {
                            destPeeker.complete(msg); // Complete to delete the message
                            destActiveCount++;
                            batchCount++;
                            operations.add("✓ Consumed DEST active: seq=" + msg.getSequenceNumber() + ", msgId=" + msg.getMessageId());
                        } catch (Exception ex) {
                            totalErrors++;
                            operations.add("✗ Failed to consume DEST active: seq=" + msg.getSequenceNumber() + ", error=" + ex.getMessage());
                        }
                    }
                    hasMoreDest = batchCount > 0;
                    if (!hasMoreDest) {
                        operations.add("DEST active messages consumed: " + destActiveCount + " total");
                    }
                }
                totalScheduledCancelled += destActiveCount; // Add to total operations
            } catch (Exception ex) {
                operations.add("✗ Error consuming DEST active messages: " + ex.getMessage());
                totalErrors++;
            }
            
            // STEP 5: Purge ALL messages from DEAD LETTER queue (poc-dead-letter)
            operations.add("STEP 5: Purging DEAD LETTER queue (poc-dead-letter)...");
            try {
                boolean hasMoreError = true;
                int errorActiveCount = 0;
                while (hasMoreError && errorActiveCount < 1000) { // Safety limit
                    IterableStream<ServiceBusReceivedMessage> errorMessages = errorReceiver.receiveMessages(32, java.time.Duration.ofSeconds(3));
                    int batchCount = 0;
                    for (ServiceBusReceivedMessage msg : errorMessages) {
                        try {
                            errorReceiver.complete(msg); // Complete to delete the error message
                            errorActiveCount++;
                            batchCount++;
                            operations.add("✓ Purged DEAD LETTER: seq=" + msg.getSequenceNumber() + ", msgId=" + msg.getMessageId());
                        } catch (Exception ex) {
                            totalErrors++;
                            operations.add("✗ Failed to purge DEAD LETTER: seq=" + msg.getSequenceNumber() + ", error=" + ex.getMessage());
                        }
                    }
                    hasMoreError = batchCount > 0;
                    if (!hasMoreError) {
                        operations.add("DEAD LETTER queue purged: " + errorActiveCount + " total");
                    }
                }
                totalScheduledCancelled += errorActiveCount; // Add to total operations
            } catch (Exception ex) {
                operations.add("✗ Error purging dead letter queue: " + ex.getMessage());
                totalErrors++;
            }
            
            // STEP 6: Clear ALL Redis tracking data
            operations.add("STEP 6: Clearing ALL Redis tracking data...");
            try {
                // Clear source tracking
                Set<String> sourceKeys = redis.keys("order:source:*");
                if (sourceKeys != null && !sourceKeys.isEmpty()) {
                    for (String key : sourceKeys) {
                        redis.delete(key);
                    }
                    totalRedisCleared += sourceKeys.size();
                    operations.add("✓ Cleared " + sourceKeys.size() + " source tracking keys");
                }
                
                // Clear destination tracking
                Set<String> destKeys = redis.keys("order:dest:*");
                if (destKeys != null && !destKeys.isEmpty()) {
                    for (String key : destKeys) {
                        redis.delete(key);
                    }
                    totalRedisCleared += destKeys.size();
                    operations.add("✓ Cleared " + destKeys.size() + " destination tracking keys");
                }
                
                // Clear transfer history
                Set<String> transferKeys = redis.keys("transfer:history:*");
                if (transferKeys != null && !transferKeys.isEmpty()) {
                    for (String key : transferKeys) {
                        redis.delete(key);
                    }
                    totalRedisCleared += transferKeys.size();
                    operations.add("✓ Cleared " + transferKeys.size() + " transfer history keys");
                }
                
                // Clear payload data
                Set<String> payloadKeys = redis.keys("order:payload:*");
                if (payloadKeys != null && !payloadKeys.isEmpty()) {
                    for (String key : payloadKeys) {
                        redis.delete(key);
                    }
                    totalRedisCleared += payloadKeys.size();
                    operations.add("✓ Cleared " + payloadKeys.size() + " payload keys");
                }
                
                // Clear archive data
                Set<String> archiveKeys = redis.keys("archive:*");
                if (archiveKeys != null && !archiveKeys.isEmpty()) {
                    for (String key : archiveKeys) {
                        redis.delete(key);
                    }
                    totalRedisCleared += archiveKeys.size();
                    operations.add("✓ Cleared " + archiveKeys.size() + " archive keys");
                }
                
            } catch (Exception ex) {
                operations.add("✗ Error clearing Redis data: " + ex.getMessage());
                totalErrors++;
            }
            
            // FINAL SUMMARY
            operations.add("=== COMPREHENSIVE CLEANUP COMPLETED ===");
            operations.add("Total messages processed: " + totalScheduledCancelled);
            operations.add("Redis keys cleared: " + totalRedisCleared);
            operations.add("Total errors: " + totalErrors);
            operations.add("Note: Includes scheduled (cancelled), active (consumed), and dead letter (purged) messages");
            
            result.put("status", totalErrors == 0 ? "completed" : "completed_with_errors");
            result.put("summary", Map.of(
                "totalMessagesProcessed", totalScheduledCancelled,
                "redisCleared", totalRedisCleared,
                "errors", totalErrors,
                "note", "Scheduled cancelled, active consumed, dead letter purged"
            ));
            result.put("totalOperations", totalScheduledCancelled + totalRedisCleared);
            result.put("operations", operations);
            result.put("timestamp", Instant.now().toString());
            
            // Log summary
            Logger.getLogger(ServiceBusSchedulerService.class.getName()).info(
                String.format("COMPREHENSIVE_CLEANUP: totalProcessed=%d, redis=%d, errors=%d",
                    totalScheduledCancelled, totalRedisCleared, totalErrors)
            );
            
        } catch (Exception ex) {
            result.put("status", "error");
            result.put("error", ex.getMessage());
            result.put("timestamp", Instant.now().toString());
            operations.add("✗ FATAL ERROR: " + ex.getMessage());
            result.put("operations", operations);
        }
        
        return result;
    }

    /**
     * Get current transfer status and queue summary
     */
    public Map<String, Object> getTransferStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        
        // Count messages in Redis tracking
        Set<String> sourceKeys = redis.keys("order:source:*");
        Set<String> destKeys = redis.keys("order:dest:*");
        Set<String> transferKeys = redis.keys("transfer:history:*");
        
        int sourceCount = sourceKeys != null ? sourceKeys.size() : 0;
        int destCount = destKeys != null ? destKeys.size() : 0;
        int transferHistory = transferKeys != null ? transferKeys.size() : 0;
        
        status.put("summary", Map.of(
            "messagesInSource", sourceCount,
            "messagesInDestination", destCount,
            "completedTransfers", transferHistory,
            "pendingTransfers", sourceCount // Messages still in source
        ));
        
        // Recent orders in source (first 5)
        List<String> recentSourceOrders = sourceKeys != null ? 
            sourceKeys.stream()
                .map(k -> k.substring("order:source:".length()))
                .limit(5)
                .toList() : 
            List.of();
            
        // Recent orders in destination (first 5)
        List<String> recentDestOrders = destKeys != null ? 
            destKeys.stream()
                .map(k -> k.substring("order:dest:".length()))
                .limit(5)
                .toList() : 
            List.of();
        
        status.put("queues", Map.of(
            "source", Map.of(
                "trackedOrders", sourceCount,
                "recentOrders", recentSourceOrders
            ),
            "destination", Map.of(
                "trackedOrders", destCount,
                "recentOrders", recentDestOrders
            )
        ));
        
        status.put("lastChecked", Instant.now().toString());
        status.put("readyForTransfer", sourceCount > 0);
        
        return status;
    }

    /**
     * Debug queue states to understand why messages go to different states
     */
    public Map<String, Object> debugQueueStates(int peekCount) {
        Map<String, Object> debug = new LinkedHashMap<>();
        
        try {
            // Get current time for reference
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            debug.put("debugTimestamp", now.toString());
            debug.put("peekCount", peekCount);
            
            // Analyze source queue
            List<Map<String, Object>> sourceMessages = peekMessages(sourcePeeker, peekCount);
            Map<String, Object> sourceAnalysis = analyzeQueueState(sourceMessages, "SOURCE", now);
            debug.put("sourceQueue", sourceAnalysis);
            
            // Analyze destination queue
            List<Map<String, Object>> destMessages = peekMessages(destPeeker, peekCount);
            Map<String, Object> destAnalysis = analyzeQueueState(destMessages, "DESTINATION", now);
            debug.put("destinationQueue", destAnalysis);
            
            // Compare Redis tracking with actual queue state
            Map<String, Object> redisComparison = compareRedisWithQueues(sourceMessages, destMessages);
            debug.put("redisComparison", redisComparison);
            
            // Timing analysis
            Map<String, Object> timingAnalysis = analyzeMessageTimings(sourceMessages, destMessages, now);
            debug.put("timingAnalysis", timingAnalysis);
            
        } catch (Exception ex) {
            debug.put("error", ex.getMessage());
            debug.put("errorType", ex.getClass().getSimpleName());
        }
        
        return debug;
    }
    
    private Map<String, Object> analyzeQueueState(List<Map<String, Object>> messages, String queueName, OffsetDateTime now) {
        Map<String, Object> analysis = new LinkedHashMap<>();
        
        analysis.put("totalMessages", messages.size());
        analysis.put("queueName", queueName);
        
        int scheduledMessages = 0;
        int activeMessages = 0;
        int pastDueMessages = 0;
        List<Map<String, Object>> messageDetails = new ArrayList<>();
        
        for (Map<String, Object> msg : messages) {
            Map<String, Object> detail = new LinkedHashMap<>();
            
            String messageId = (String) msg.get("messageId");
            Object scheduledTimeObj = msg.get("scheduledEnqueueTime");
            Long sequenceNumber = (Long) msg.get("sequenceNumber");
            
            detail.put("messageId", messageId);
            detail.put("sequenceNumber", sequenceNumber);
            
            if (scheduledTimeObj instanceof OffsetDateTime) {
                OffsetDateTime scheduledTime = (OffsetDateTime) scheduledTimeObj;
                detail.put("scheduledFor", scheduledTime.toString());
                
                long minutesUntilScheduled = java.time.Duration.between(now, scheduledTime).toMinutes();
                detail.put("minutesUntilScheduled", minutesUntilScheduled);
                
                if (minutesUntilScheduled <= 0) {
                    pastDueMessages++;
                    detail.put("status", "PAST_DUE");
                    detail.put("overdueByMinutes", Math.abs(minutesUntilScheduled));
                } else {
                    scheduledMessages++;
                    detail.put("status", "SCHEDULED");
                }
            } else {
                activeMessages++;
                detail.put("status", "ACTIVE");
                detail.put("scheduledFor", "null - Active message");
            }
            
            // Add application properties for context
            @SuppressWarnings("unchecked")
            Map<String, Object> appProps = (Map<String, Object>) msg.get("applicationProperties");
            if (appProps != null) {
                detail.put("originalScheduledTime", appProps.get("originalScheduledTime"));
                detail.put("transferredFrom", appProps.get("transferredFrom"));
                detail.put("brand", appProps.get("brand"));
            }
            
            messageDetails.add(detail);
        }
        
        analysis.put("messageBreakdown", Map.of(
            "scheduled", scheduledMessages,
            "active", activeMessages,
            "pastDue", pastDueMessages
        ));
        
        analysis.put("messages", messageDetails);
        
        return analysis;
    }
    
    private Map<String, Object> compareRedisWithQueues(List<Map<String, Object>> sourceMessages, List<Map<String, Object>> destMessages) {
        Map<String, Object> comparison = new LinkedHashMap<>();
        
        // Get Redis tracking
        Set<String> sourceKeys = redis.keys("order:source:*");
        Set<String> destKeys = redis.keys("order:dest:*");
        
        List<String> redisSourceOrders = sourceKeys != null ? 
            sourceKeys.stream().map(k -> k.substring("order:source:".length())).toList() : List.of();
        List<String> redisDestOrders = destKeys != null ? 
            destKeys.stream()
                .filter(k -> !k.contains(":payload:")) // Exclude payload keys
                .map(k -> k.substring("order:dest:".length()))
                .toList() : List.of();
            
        // Get actual queue message IDs
        List<String> actualSourceMessages = sourceMessages.stream()
            .map(m -> (String) m.get("messageId"))
            .filter(id -> id != null)
            .toList();
        List<String> actualDestMessages = destMessages.stream()
            .map(m -> (String) m.get("messageId"))
            .filter(id -> id != null)
            .toList();
            
        comparison.put("redis", Map.of(
            "sourceOrders", redisSourceOrders,
            "destOrders", redisDestOrders
        ));
        
        comparison.put("actualQueues", Map.of(
            "sourceMessages", actualSourceMessages,
            "destMessages", actualDestMessages
        ));
        
        // Find mismatches
        List<String> redisButNotInSourceQueue = redisSourceOrders.stream()
            .filter(id -> !actualSourceMessages.contains(id))
            .toList();
        List<String> inSourceQueueButNotRedis = actualSourceMessages.stream()
            .filter(id -> !redisSourceOrders.contains(id))
            .toList();
            
        comparison.put("mismatches", Map.of(
            "redisSourceButNotInQueue", redisButNotInSourceQueue,
            "inSourceQueueButNotRedis", inSourceQueueButNotRedis,
            "redisDestCount", redisDestOrders.size(),
            "actualDestCount", actualDestMessages.size()
        ));
        
        return comparison;
    }
    
    private Map<String, Object> analyzeMessageTimings(List<Map<String, Object>> sourceMessages, 
                                                      List<Map<String, Object>> destMessages, 
                                                      OffsetDateTime now) {
        Map<String, Object> timing = new LinkedHashMap<>(); 
        
        // Analyze timing patterns
        List<Map<String, Object>> timingDetails = new ArrayList<>();
        
        // Source messages timing
        for (Map<String, Object> msg : sourceMessages) {
            Object scheduledTimeObj = msg.get("scheduledEnqueueTime");
            if (scheduledTimeObj instanceof OffsetDateTime) {
                OffsetDateTime scheduledTime = (OffsetDateTime) scheduledTimeObj;
                Map<String, Object> detail = Map.of(
                    "queue", "SOURCE",
                    "messageId", msg.get("messageId"),
                    "scheduledFor", scheduledTime.toString(),
                    "minutesFromNow", java.time.Duration.between(now, scheduledTime).toMinutes(),
                    "isOverdue", scheduledTime.isBefore(now)
                );
                timingDetails.add(detail);
            }
        }
        
        // Destination messages timing
        for (Map<String, Object> msg : destMessages) {
            Object scheduledTimeObj = msg.get("scheduledEnqueueTime");
            if (scheduledTimeObj instanceof OffsetDateTime) {
                OffsetDateTime scheduledTime = (OffsetDateTime) scheduledTimeObj;
                Map<String, Object> detail = Map.of(
                    "queue", "DESTINATION",
                    "messageId", msg.get("messageId"),
                    "scheduledFor", scheduledTime.toString(),
                    "minutesFromNow", java.time.Duration.between(now, scheduledTime).toMinutes(),
                    "isOverdue", scheduledTime.isBefore(now)
                );
                timingDetails.add(detail);
            }
        }
        
        timing.put("messageTimings", timingDetails);
        timing.put("currentTime", now.toString());
        
        return timing;
    }
    
    /**
     * Analyze timing patterns from Redis transfer history
     */
    public Map<String, Object> debugTimingAnalysis() {
        Map<String, Object> analysis = new LinkedHashMap<>();
        
        Set<String> transferKeys = redis.keys("transfer:history:*");
        if (transferKeys == null || transferKeys.isEmpty()) {
            analysis.put("message", "No transfer history found");
            return analysis;
        }
        
        List<Map<String, Object>> transferAnalysis = new ArrayList<>();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        
        for (String transferKey : transferKeys) {
            try {
                String historyJson = redis.opsForValue().get(transferKey);
                if (historyJson != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> history = mapper.readValue(historyJson, Map.class);
                    
                    String orderId = transferKey.substring("transfer:history:".length());
                    String originalScheduledTime = (String) history.get("originalScheduledTime");
                    String transferredAt = (String) history.get("transferredAt");
                    
                    Map<String, Object> detail = new LinkedHashMap<>();
                    detail.put("orderId", orderId);
                    detail.put("originalScheduledTime", originalScheduledTime);
                    detail.put("transferredAt", transferredAt);
                    
                    if (originalScheduledTime != null && transferredAt != null) {
                        try {
                            OffsetDateTime original = OffsetDateTime.parse(originalScheduledTime);
                            OffsetDateTime transferred = OffsetDateTime.parse(transferredAt);
                            
                            long minutesUntilOriginalSchedule = java.time.Duration.between(now, original).toMinutes();
                            long minutesSinceTransfer = java.time.Duration.between(transferred, now).toMinutes();
                            
                            detail.put("minutesUntilOriginalSchedule", minutesUntilOriginalSchedule);
                            detail.put("minutesSinceTransfer", minutesSinceTransfer);
                            detail.put("wasOverdueAtTransfer", original.isBefore(transferred));
                            detail.put("shouldBeActiveNow", original.isBefore(now));
                        } catch (Exception ex) {
                            detail.put("timingParseError", ex.getMessage());
                        }
                    }
                    
                    transferAnalysis.add(detail);
                }
            } catch (Exception ex) {
                transferAnalysis.add(Map.of(
                    "orderId", transferKey.substring("transfer:history:".length()),
                    "error", ex.getMessage()
                ));
            }
        }
        
        analysis.put("transferHistory", transferAnalysis);
        analysis.put("totalTransfers", transferAnalysis.size());
        analysis.put("analyzedAt", now.toString());
        
        return analysis;
    }

    /**
     * Purge Redis keys by pattern - useful for cleanup and testing
     */
    public Map<String, Object> purgeRedisKeys(String pattern, boolean includeArchive) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> deletedKeys = new ArrayList<>();
        int totalDeleted = 0;
        
        try {
            // Get keys matching pattern
            Set<String> keys = redis.keys(pattern);
            if (keys != null && !keys.isEmpty()) {
                for (String key : keys) {
                    // Skip archive keys unless explicitly requested
                    if (!includeArchive && key.startsWith("archive:")) {
                        continue;
                    }
                    
                    redis.delete(key);
                    deletedKeys.add(key);
                    totalDeleted++;
                }
            }
            
            // If including archive, also clean archive keys
            if (includeArchive) {
                Set<String> archiveKeys = redis.keys("archive:*");
                if (archiveKeys != null) {
                    for (String key : archiveKeys) {
                        redis.delete(key);
                        deletedKeys.add(key);
                        totalDeleted++;
                    }
                }
            }
            
            result.put("status", "success");
            result.put("pattern", pattern);
            result.put("includeArchive", includeArchive);
            result.put("totalDeleted", totalDeleted);
            result.put("deletedKeys", deletedKeys);
            result.put("timestamp", Instant.now().toString());
            
        } catch (Exception ex) {
            result.put("status", "error");
            result.put("error", ex.getMessage());
            result.put("timestamp", Instant.now().toString());
        }
        
        return result;
    }
    
    /**
     * List Redis keys by pattern for debugging
     */
    public Map<String, Object> listRedisKeys(String pattern) {
        Map<String, Object> result = new LinkedHashMap<>();
        
        try {
            Set<String> keys = redis.keys(pattern);
            List<Map<String, Object>> keyDetails = new ArrayList<>();
            
            if (keys != null) {
                for (String key : keys) {
                    Map<String, Object> detail = new LinkedHashMap<>();
                    detail.put("key", key);
                    detail.put("type", redis.type(key).toString());
                    detail.put("ttl", redis.getExpire(key));
                    
                    // Get key content based on type
                    if (key.startsWith("order:") && redis.type(key).toString().equals("HASH")) {
                        detail.put("data", redis.opsForHash().entries(key));
                    } else if (redis.type(key).toString().equals("STRING")) {
                        detail.put("value", redis.opsForValue().get(key));
                    }
                    
                    keyDetails.add(detail);
                }
            }
            
            result.put("pattern", pattern);
            result.put("totalKeys", keys != null ? keys.size() : 0);
            result.put("keys", keyDetails);
            result.put("timestamp", Instant.now().toString());
            
        } catch (Exception ex) {
            result.put("error", ex.getMessage());
            result.put("timestamp", Instant.now().toString());
        }
        
        return result;
    }

    /**
     * Redis crash recovery - rebuild tracking from Azure Service Bus queues
     * Call this when Redis comes back online after a crash
     */
    public Map<String, Object> rebuildRedisFromQueues(int maxMessages) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> operations = new ArrayList<>();
        int sourceRebuilt = 0;
        int destRebuilt = 0;
        int errors = 0;
        
        try {
            operations.add("=== STARTING REDIS RECOVERY ===");
            
            // 1. Rebuild source queue tracking
            operations.add("Scanning source queue for scheduled messages...");
            List<Map<String, Object>> sourceMessages = peekMessages(sourcePeeker, maxMessages);
            
            for (Map<String, Object> msg : sourceMessages) {
                try {
                    String messageId = (String) msg.get("messageId");
                    Long sequenceNumber = (Long) msg.get("sequenceNumber");
                    Object scheduledTimeObj = msg.get("scheduledEnqueueTime");
                    
                    if (messageId != null && sequenceNumber != null && scheduledTimeObj instanceof OffsetDateTime) {
                        OffsetDateTime scheduledTime = (OffsetDateTime) scheduledTimeObj;
                        
                        // Only rebuild scheduled messages (not past due)
                        if (scheduledTime.isAfter(OffsetDateTime.now(ZoneOffset.UTC))) {
                            String sourceKey = "order:source:" + messageId;
                            Map<String, String> orderData = Map.of(
                                "sequenceNumber", sequenceNumber.toString(),
                                "scheduledFor", scheduledTime.toString(),
                                "recoveredAt", Instant.now().toString(),
                                "recoverySource", "source_queue_scan"
                            );
                            
                            redis.opsForHash().putAll(sourceKey, orderData);
                            redis.expire(sourceKey, 7, TimeUnit.DAYS);
                            
                            sourceRebuilt++;
                            operations.add("Rebuilt source tracking: " + messageId + " (seq: " + sequenceNumber + ")");
                        }
                    }
                } catch (Exception ex) {
                    errors++;
                    operations.add("Error rebuilding source message: " + ex.getMessage());
                }
            }
            
            // 2. Rebuild destination queue tracking  
            operations.add("Scanning destination queue for scheduled messages...");
            List<Map<String, Object>> destMessages = peekMessages(destPeeker, maxMessages);
            
            for (Map<String, Object> msg : destMessages) {
                try {
                    String messageId = (String) msg.get("messageId");
                    Long sequenceNumber = (Long) msg.get("sequenceNumber");
                    Object scheduledTimeObj = msg.get("scheduledEnqueueTime");
                    
                    @SuppressWarnings("unchecked")
                    Map<String, Object> appProps = (Map<String, Object>) msg.get("applicationProperties");
                    
                    if (messageId != null && sequenceNumber != null && scheduledTimeObj instanceof OffsetDateTime) {
                        OffsetDateTime scheduledTime = (OffsetDateTime) scheduledTimeObj;
                        
                        // Only rebuild scheduled messages
                        if (scheduledTime.isAfter(OffsetDateTime.now(ZoneOffset.UTC))) {
                            String destKey = "order:dest:" + messageId;
                            Map<String, String> destData = new LinkedHashMap<>();
                            destData.put("sequenceNumber", sequenceNumber.toString());
                            destData.put("scheduledFor", scheduledTime.toString());
                            destData.put("recoveredAt", Instant.now().toString());
                            destData.put("recoverySource", "dest_queue_scan");
                            
                            // Include transfer metadata if available
                            if (appProps != null) {
                                if (appProps.get("transferredAt") != null) {
                                    destData.put("transferredAt", appProps.get("transferredAt").toString());
                                }
                                if (appProps.get("originalSequence") != null) {
                                    destData.put("fromSequence", appProps.get("originalSequence").toString());
                                }
                            }
                            
                            redis.opsForHash().putAll(destKey, destData);
                            redis.expire(destKey, 7, TimeUnit.DAYS);
                            
                            destRebuilt++;
                            operations.add("Rebuilt dest tracking: " + messageId + " (seq: " + sequenceNumber + ")");
                        }
                    }
                } catch (Exception ex) {
                    errors++;
                    operations.add("Error rebuilding dest message: " + ex.getMessage());
                }
            }
            
            operations.add("=== RECOVERY COMPLETE ===");
            operations.add("Source rebuilt: " + sourceRebuilt + ", Dest rebuilt: " + destRebuilt + ", Errors: " + errors);
            
            result.put("status", "completed");
            result.put("sourceRebuilt", sourceRebuilt);
            result.put("destRebuilt", destRebuilt);
            result.put("errors", errors);
            result.put("operations", operations);
            result.put("timestamp", Instant.now().toString());
            
        } catch (Exception ex) {
            result.put("status", "error");
            result.put("error", ex.getMessage());
            result.put("timestamp", Instant.now().toString());
        }
        
        return result;
    }

    /**
     * Check dead letter queue status and peek messages
     */
    public Map<String, Object> checkDeadLetterQueue(int peekCount) {
        Map<String, Object> deadLetterStatus = new LinkedHashMap<>();
        
        try {
            // Peek dead letter messages
            List<Map<String, Object>> deadLetterMessages = new ArrayList<>();
            IterableStream<ServiceBusReceivedMessage> peekedMessages = errorReceiver.peekMessages(Math.min(peekCount, 100));
            
            for (ServiceBusReceivedMessage msg : peekedMessages) {
                Map<String, Object> msgInfo = new LinkedHashMap<>();
                msgInfo.put("sequenceNumber", msg.getSequenceNumber());
                msgInfo.put("messageId", msg.getMessageId());
                msgInfo.put("correlationId", msg.getCorrelationId());
                msgInfo.put("enqueuedTime", msg.getEnqueuedTime());
                msgInfo.put("deliveryCount", msg.getDeliveryCount());
                msgInfo.put("deadLetterReason", msg.getDeadLetterReason());
                msgInfo.put("deadLetterErrorDescription", msg.getDeadLetterErrorDescription());
                msgInfo.put("applicationProperties", new LinkedHashMap<>(msg.getApplicationProperties()));
                
                // Add body preview (first 200 chars)
                String bodyPreview = msg.getBody() != null ? msg.getBody().toString() : "null";
                if (bodyPreview.length() > 200) {
                    bodyPreview = bodyPreview.substring(0, 200) + "...";
                }
                msgInfo.put("bodyPreview", bodyPreview);
                
                deadLetterMessages.add(msgInfo);
            }
            
            deadLetterStatus.put("status", "success");
            deadLetterStatus.put("messageCount", deadLetterMessages.size());
            deadLetterStatus.put("messages", deadLetterMessages);
            deadLetterStatus.put("queueName", "poc-dead-letter");
            
        } catch (Exception ex) {
            deadLetterStatus.put("status", "error");
            deadLetterStatus.put("error", ex.getMessage());
            deadLetterStatus.put("messageCount", 0);
            deadLetterStatus.put("messages", List.of());
        }
        
        deadLetterStatus.put("timestamp", Instant.now().toString());
        return deadLetterStatus;
    }
}

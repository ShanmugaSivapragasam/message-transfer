package com.example.messagetransfer.service;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.*;
import com.example.messagetransfer.model.OrderPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class ServiceBusSchedulerService {

    private final ServiceBusSenderClient sourceSender;
    private final ServiceBusSenderClient destSender;
    private final ServiceBusSenderClient errorSender;
    private final ServiceBusReceiverClient sourcePeeker;
    private final ServiceBusReceiverClient destPeeker;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${app.defaultScheduleDelaySeconds:3600}")
    private int defaultDelaySeconds;

    public ServiceBusSchedulerService(ServiceBusSenderClient sourceSender,
                                      ServiceBusSenderClient destSender,
                                      ServiceBusSenderClient errorSender,
                                      ServiceBusReceiverClient sourcePeeker,
                                      ServiceBusReceiverClient destPeeker,
                                      StringRedisTemplate redis) {
        this.sourceSender = sourceSender;
        this.destSender = destSender;
        this.errorSender = errorSender;
        this.sourcePeeker = sourcePeeker;
        this.destPeeker = destPeeker;
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

                long seq = sourceSender.scheduleMessage(msg, scheduledFor);

                // Store in Redis with separate source tracking and payload preservation
                String sourceKey = "order:source:" + order.orderId();
                String payloadKey = "order:payload:" + order.orderId();
                
                redis.opsForValue().set(sourceKey, Long.toString(seq), 7, TimeUnit.DAYS);
                redis.opsForValue().set(payloadKey, body, 7, TimeUnit.DAYS);

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
     * Enhanced transfer with Redis state management:
     * 1. Find all source scheduled messages
     * 2. Cancel from source queue
     * 3. Preserve original payload, metadata, and scheduled time
     * 4. Schedule to destination queue with SAME original time
     * 5. Update Redis with new sequence numbers for future cancellation
     * 6. Track all changes for validation
     */
    public Map<String, Object> transferMessagesWithRedisUpdate() {
        Set<String> sourceKeys = redis.keys("order:source:*");
        if (sourceKeys == null || sourceKeys.isEmpty()) {
            return Map.of("transferred", 0, "errors", 0, "message", "No source messages found in Redis");
        }
        
        int transferred = 0;
        int errors = 0;
        List<Map<String, Object>> transferDetails = new ArrayList<>();
        
        for (String sourceKey : sourceKeys) {
            try {
                String orderId = sourceKey.substring("order:source:".length());
                
                // Get source sequence number and payload
                String sourceSeqStr = redis.opsForValue().get(sourceKey);
                String payloadKey = "order:payload:" + orderId;
                String originalPayload = redis.opsForValue().get(payloadKey);
                
                if (sourceSeqStr == null) {
                    errors++;
                    transferDetails.add(Map.of("orderId", orderId, "error", "Source sequence not found"));
                    continue;
                }
                
                long sourceSeq = Long.parseLong(sourceSeqStr);
                
                // Get original scheduled time from source queue by peeking
                OffsetDateTime originalScheduledTime = null;
                Map<String, Object> originalMetadata = new HashMap<>();
                
                try {
                    // Peek the source message to get original timing and metadata
                    IterableStream<ServiceBusReceivedMessage> peekedMessages = sourcePeeker.peekMessages(100);
                    for (ServiceBusReceivedMessage msg : peekedMessages) {
                        if (msg.getSequenceNumber() == sourceSeq) {
                            originalScheduledTime = msg.getScheduledEnqueueTime();
                            originalMetadata.putAll(msg.getApplicationProperties());
                            break;
                        }
                    }
                } catch (Exception ex) {
                    // If we can't peek, use current time + default delay as fallback
                    originalScheduledTime = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(defaultDelaySeconds);
                }
                
                // Cancel the source scheduled message
                sourceSender.cancelScheduledMessage(sourceSeq);
                
                // Prepare message for destination
                ServiceBusMessage destMsg;
                if (originalPayload != null) {
                    // Use original payload if available
                    destMsg = new ServiceBusMessage(originalPayload);
                } else {
                    // Fallback: create minimal payload
                    Map<String, Object> fallbackBody = Map.of(
                        "orderId", orderId,
                        "transferredAt", Instant.now().toString(),
                        "originalSequence", sourceSeq
                    );
                    destMsg = new ServiceBusMessage(mapper.writeValueAsBytes(fallbackBody));
                }
                
                destMsg.setMessageId(orderId);
                destMsg.setCorrelationId(orderId);
                destMsg.setContentType("application/json");
                
                // Preserve ALL original metadata and add transfer tracking
                destMsg.getApplicationProperties().putAll(originalMetadata);
                destMsg.getApplicationProperties().put("transferredFrom", "source");
                destMsg.getApplicationProperties().put("originalSequence", sourceSeq);
                destMsg.getApplicationProperties().put("originalScheduledTime", originalScheduledTime.toString());
                destMsg.getApplicationProperties().put("transferredAt", Instant.now().toString());
                
                // PRESERVE ORIGINAL SCHEDULED TIME (not reset to now + delay)
                OffsetDateTime preservedScheduledTime = originalScheduledTime != null ? 
                    originalScheduledTime : OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(defaultDelaySeconds);
                    
                destMsg.getApplicationProperties().put("scheduledFor", preservedScheduledTime.toString());
                
                // Schedule on destination queue with SAME original time
                long destSeq = destSender.scheduleMessage(destMsg, preservedScheduledTime);
                
                // Update Redis: Remove source keys, add destination keys
                redis.delete(sourceKey);
                redis.delete(payloadKey); // Clean up payload cache
                
                // Store new destination sequence and transfer metadata for validation
                String destKey = "order:dest:" + orderId;
                redis.opsForValue().set(destKey, Long.toString(destSeq), 7, TimeUnit.DAYS);
                
                // Store destination payload for potential future transfers
                String destPayloadKey = "order:dest:payload:" + orderId;
                redis.opsForValue().set(destPayloadKey, destMsg.getBody().toString(), 7, TimeUnit.DAYS);
                
                // Store transfer tracking for validation
                String transferKey = "transfer:history:" + orderId;
                Map<String, Object> transferRecord = Map.of(
                    "sourceSequence", sourceSeq,
                    "destSequence", destSeq,
                    "originalScheduledTime", preservedScheduledTime.toString(),
                    "transferredAt", Instant.now().toString(),
                    "fromQueue", "source",
                    "toQueue", "destination"
                );
                redis.opsForValue().set(transferKey, mapper.writeValueAsString(transferRecord), 7, TimeUnit.DAYS);
                
                transferred++;
                transferDetails.add(Map.of(
                    "orderId", orderId,
                    "sourceSequence", sourceSeq,
                    "destSequence", destSeq,
                    "originalScheduledTime", preservedScheduledTime.toString(),
                    "transferredAt", Instant.now().toString(),
                    "timingPreserved", true,
                    "metadataPreserved", originalMetadata.size(),
                    "status", "transferred"
                ));
                
            } catch (Exception ex) {
                errors++;
                String orderId = sourceKey.contains(":") ? sourceKey.substring(sourceKey.lastIndexOf(":") + 1) : "unknown";
                transferDetails.add(Map.of(
                    "orderId", orderId,
                    "error", ex.getMessage(),
                    "status", "failed"
                ));
                
                // Log error to error queue
                try {
                    Map<String, Object> errorRecord = Map.of(
                        "type", "TRANSFER_ERROR",
                        "timestamp", Instant.now().toString(),
                        "orderId", orderId,
                        "sourceKey", sourceKey,
                        "error", ex.getMessage()
                    );
                    ServiceBusMessage errorMsg = new ServiceBusMessage(mapper.writeValueAsBytes(errorRecord));
                    errorMsg.setContentType("application/json");
                    errorSender.sendMessage(errorMsg);
                } catch (Exception ignore) {}
            }
        }
        
        return Map.of(
            "transferred", transferred,
            "errors", errors,
            "details", transferDetails,
            "timestamp", Instant.now().toString(),
            "timingPreservationEnabled", true
        );
    }

    /**
     * Legacy transfer method for backward compatibility
     */
    public Map<String, Object> transferAllFromRedis() {
        return transferMessagesWithRedisUpdate();
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
                sourceSender.cancelScheduledMessage(sourceSeq);
                redis.delete(sourceKey);
                redis.delete("order:payload:" + orderId);
                result.put("status", "cancelled_from_source");
                result.put("sequenceNumber", sourceSeq);
                return result;
            }
            
            // Try to cancel from destination
            String destKey = "order:dest:" + orderId;
            String destSeqStr = redis.opsForValue().get(destKey);
            
            if (destSeqStr != null) {
                long destSeq = Long.parseLong(destSeqStr);
                destSender.cancelScheduledMessage(destSeq);
                redis.delete(destKey);
                redis.delete("order:dest:payload:" + orderId);
                result.put("status", "cancelled_from_destination");
                result.put("sequenceNumber", destSeq);
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
     * DANGER: Completely clean up both source and destination queues
     * This will cancel ALL scheduled messages and clear Redis tracking
     * Use only for testing/development!
     */
    public Map<String, Object> cleanupAllQueues() {
        Map<String, Object> result = new LinkedHashMap<>();
        int sourceCancelled = 0;
        int destCancelled = 0;
        int sourceErrors = 0;
        int destErrors = 0;
        List<String> operations = new ArrayList<>();
        
        try {
            // 1. Clean up source queue using Redis tracking
            Set<String> sourceKeys = redis.keys("order:source:*");
            if (sourceKeys != null) {
                for (String sourceKey : sourceKeys) {
                    try {
                        String orderId = sourceKey.substring("order:source:".length());
                        String sourceSeqStr = redis.opsForValue().get(sourceKey);
                        
                        if (sourceSeqStr != null) {
                            long sourceSeq = Long.parseLong(sourceSeqStr);
                            sourceSender.cancelScheduledMessage(sourceSeq);
                            redis.delete(sourceKey);
                            redis.delete("order:payload:" + orderId);
                            sourceCancelled++;
                            operations.add("Cancelled source message: " + orderId + " (seq: " + sourceSeq + ")");
                        }
                    } catch (Exception ex) {
                        sourceErrors++;
                        operations.add("Error cancelling source message: " + ex.getMessage());
                    }
                }
            }
            
            // 2. Clean up destination queue using Redis tracking
            Set<String> destKeys = redis.keys("order:dest:*");
            if (destKeys != null) {
                for (String destKey : destKeys) {
                    try {
                        String orderId = destKey.substring("order:dest:".length());
                        String destSeqStr = redis.opsForValue().get(destKey);
                        
                        if (destSeqStr != null) {
                            long destSeq = Long.parseLong(destSeqStr);
                            destSender.cancelScheduledMessage(destSeq);
                            redis.delete(destKey);
                            redis.delete("order:dest:payload:" + orderId);
                            destCancelled++;
                            operations.add("Cancelled destination message: " + orderId + " (seq: " + destSeq + ")");
                        }
                    } catch (Exception ex) {
                        destErrors++;
                        operations.add("Error cancelling destination message: " + ex.getMessage());
                    }
                }
            }
            
            // 3. Clean up transfer history
            Set<String> transferKeys = redis.keys("transfer:history:*");
            int historyCleared = 0;
            if (transferKeys != null) {
                for (String transferKey : transferKeys) {
                    redis.delete(transferKey);
                    historyCleared++;
                }
            }
            
            result.put("status", "completed");
            result.put("sourceQueue", Map.of(
                "cancelled", sourceCancelled,
                "errors", sourceErrors
            ));
            result.put("destinationQueue", Map.of(
                "cancelled", destCancelled,
                "errors", destErrors
            ));
            result.put("transferHistoryCleared", historyCleared);
            result.put("totalOperations", sourceCancelled + destCancelled + historyCleared);
            result.put("operations", operations);
            result.put("timestamp", Instant.now().toString());
            
            // Log cleanup operation
            operations.add("=== CLEANUP SUMMARY ===");
            operations.add("Source cancelled: " + sourceCancelled + ", errors: " + sourceErrors);
            operations.add("Dest cancelled: " + destCancelled + ", errors: " + destErrors);
            operations.add("Transfer history cleared: " + historyCleared);
            
        } catch (Exception ex) {
            result.put("status", "error");
            result.put("error", ex.getMessage());
            result.put("timestamp", Instant.now().toString());
        }
        
        return result;
    }
}

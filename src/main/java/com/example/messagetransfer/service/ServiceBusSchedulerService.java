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
     * 3. Preserve original payload and metadata
     * 4. Schedule to destination queue
     * 5. Update Redis with new sequence numbers for future cancellation
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
                destMsg.getApplicationProperties().put("transferredFrom", "source");
                destMsg.getApplicationProperties().put("originalSequence", sourceSeq);
                destMsg.getApplicationProperties().put("transferredAt", Instant.now().toString());
                
                OffsetDateTime destScheduledFor = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(defaultDelaySeconds);
                destMsg.getApplicationProperties().put("scheduledFor", destScheduledFor.toString());
                
                // Schedule on destination queue
                long destSeq = destSender.scheduleMessage(destMsg, destScheduledFor);
                
                // Update Redis: Remove source keys, add destination keys
                redis.delete(sourceKey);
                redis.delete(payloadKey); // Clean up payload cache
                
                // Store new destination sequence for future cancellation
                String destKey = "order:dest:" + orderId;
                redis.opsForValue().set(destKey, Long.toString(destSeq), 7, TimeUnit.DAYS);
                
                // Store destination payload for potential future transfers
                String destPayloadKey = "order:dest:payload:" + orderId;
                redis.opsForValue().set(destPayloadKey, destMsg.getBody().toString(), 7, TimeUnit.DAYS);
                
                transferred++;
                transferDetails.add(Map.of(
                    "orderId", orderId,
                    "sourceSequence", sourceSeq,
                    "destSequence", destSeq,
                    "scheduledFor", destScheduledFor.toString(),
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
            "timestamp", Instant.now().toString()
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
     * Get order information from Redis
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
        
        if (sourceSeq == null && destSeq == null) {
            info.put("status", "not_found");
        } else {
            info.put("status", "tracked");
        }
        
        return info;
    }
}

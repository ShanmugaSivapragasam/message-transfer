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

                // Store orderId -> sequenceNumber in Redis for cancellation/transfer
                redis.opsForValue().set(redisKey(order.orderId()), Long.toString(seq), 7, TimeUnit.DAYS);

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

    public Map<String, Object> transferAllFromRedis() {
        Set<String> keys = redis.keys("order:seq:*");
        if (keys == null || keys.isEmpty()) {
            return Map.of("transferred", 0, "errors", 0);
        }
        int ok = 0;
        int err = 0;
        for (String key : keys) {
            try {
                String orderId = key.substring("order:seq:".length());
                long seq = Long.parseLong(Objects.requireNonNull(redis.opsForValue().get(key)));

                // Cancel on source (using the stored scheduled sequence number)
                sourceSender.cancelScheduledMessage(seq);

                // Minimal body for POC transfer (real impl would persist original payload)
                Map<String, Object> body = Map.of(
                        "orderId", orderId,
                        "transferredAt", Instant.now().toString()
                );
                ServiceBusMessage msg = new ServiceBusMessage(mapper.writeValueAsBytes(body));
                msg.setMessageId(orderId);
                msg.setCorrelationId(orderId);
                msg.setContentType("application/json");
                msg.getApplicationProperties().put("brand", "generic");
                msg.getApplicationProperties().put("channel", "app");
                msg.getApplicationProperties().put("version", "1.0.0");

                OffsetDateTime scheduledFor = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(defaultDelaySeconds);
                msg.getApplicationProperties().put("scheduledFor", scheduledFor.toString());

                destSender.scheduleMessage(msg, scheduledFor);

                // Remove key upon success
                redis.delete(key);
                ok++;
            } catch (Exception ex) {
                err++;
                try {
                    Map<String, Object> errorRecord = Map.of(
                            "type", "TRANSFER_ERROR",
                            "ts", Instant.now().toString(),
                            "key", key,
                            "error", ex.getMessage()
                    );
                    ServiceBusMessage em = new ServiceBusMessage(mapper.writeValueAsBytes(errorRecord));
                    em.setContentType("application/json");
                    errorSender.sendMessage(em);
                } catch (Exception ignore) {}
            }
        }
        return Map.of("transferred", ok, "errors", err);
    }

    private String redisKey(String orderId) {
        return "order:seq:" + orderId;
    }
}

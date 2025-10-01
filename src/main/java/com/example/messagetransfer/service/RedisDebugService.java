package com.example.messagetransfer.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Redis Debug Service for inspecting Redis state during development
 */
@Service
public class RedisDebugService {

    private final StringRedisTemplate redis;

    public RedisDebugService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Get all Redis keys and their values
     */
    public Map<String, Object> getAllKeys() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        // Get source keys
        Set<String> sourceKeys = redis.keys("order:source:*");
        Map<String, String> sourceData = new LinkedHashMap<>();
        if (sourceKeys != null) {
            for (String key : sourceKeys) {
                sourceData.put(key, redis.opsForValue().get(key));
            }
        }
        
        // Get destination keys
        Set<String> destKeys = redis.keys("order:dest:*");
        Map<String, String> destData = new LinkedHashMap<>();
        if (destKeys != null) {
            for (String key : destKeys) {
                destData.put(key, redis.opsForValue().get(key));
            }
        }
        
        // Get payload keys
        Set<String> payloadKeys = redis.keys("order:payload:*");
        Map<String, String> payloadData = new LinkedHashMap<>();
        if (payloadKeys != null) {
            for (String key : payloadKeys) {
                String value = redis.opsForValue().get(key);
                // Truncate long payloads for readability
                if (value != null && value.length() > 200) {
                    value = value.substring(0, 200) + "... (truncated)";
                }
                payloadData.put(key, value);
            }
        }
        
        // Get destination payload keys
        Set<String> destPayloadKeys = redis.keys("order:dest:payload:*");
        Map<String, String> destPayloadData = new LinkedHashMap<>();
        if (destPayloadKeys != null) {
            for (String key : destPayloadKeys) {
                String value = redis.opsForValue().get(key);
                if (value != null && value.length() > 200) {
                    value = value.substring(0, 200) + "... (truncated)";
                }
                destPayloadData.put(key, value);
            }
        }
        
        result.put("sourceSequences", sourceData);
        result.put("destSequences", destData);
        result.put("sourcePayloads", payloadData);
        result.put("destPayloads", destPayloadData);
        result.put("totalKeys", sourceData.size() + destData.size() + payloadData.size() + destPayloadData.size());
        result.put("timestamp", java.time.Instant.now().toString());
        
        return result;
    }

    /**
     * Get summary statistics
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        
        Set<String> sourceKeys = redis.keys("order:source:*");
        Set<String> destKeys = redis.keys("order:dest:*");
        Set<String> payloadKeys = redis.keys("order:payload:*");
        Set<String> destPayloadKeys = redis.keys("order:dest:payload:*");
        
        stats.put("sourceOrders", sourceKeys != null ? sourceKeys.size() : 0);
        stats.put("destOrders", destKeys != null ? destKeys.size() : 0);
        stats.put("sourcePayloads", payloadKeys != null ? payloadKeys.size() : 0);
        stats.put("destPayloads", destPayloadKeys != null ? destPayloadKeys.size() : 0);
        stats.put("timestamp", java.time.Instant.now().toString());
        
        return stats;
    }

    /**
     * Clear all order-related Redis keys (for testing)
     */
    public Map<String, Object> clearAll() {
        Set<String> allOrderKeys = new HashSet<>();
        
        Set<String> sourceKeys = redis.keys("order:source:*");
        Set<String> destKeys = redis.keys("order:dest:*");
        Set<String> payloadKeys = redis.keys("order:payload:*");
        Set<String> destPayloadKeys = redis.keys("order:dest:payload:*");
        
        if (sourceKeys != null) allOrderKeys.addAll(sourceKeys);
        if (destKeys != null) allOrderKeys.addAll(destKeys);
        if (payloadKeys != null) allOrderKeys.addAll(payloadKeys);
        if (destPayloadKeys != null) allOrderKeys.addAll(destPayloadKeys);
        
        int deletedCount = 0;
        for (String key : allOrderKeys) {
            if (redis.delete(key)) {
                deletedCount++;
            }
        }
        
        return Map.of(
            "deletedKeys", deletedCount,
            "totalKeysFound", allOrderKeys.size(),
            "timestamp", java.time.Instant.now().toString()
        );
    }

    /**
     * Get specific order details by orderId
     */
    public Map<String, Object> getOrderDetails(String orderId) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("orderId", orderId);
        
        // Check all possible keys for this order
        String sourceKey = "order:source:" + orderId;
        String destKey = "order:dest:" + orderId;
        String payloadKey = "order:payload:" + orderId;
        String destPayloadKey = "order:dest:payload:" + orderId;
        
        String sourceSeq = redis.opsForValue().get(sourceKey);
        String destSeq = redis.opsForValue().get(destKey);
        String payload = redis.opsForValue().get(payloadKey);
        String destPayload = redis.opsForValue().get(destPayloadKey);
        
        if (sourceSeq != null) {
            details.put("sourceSequence", Long.parseLong(sourceSeq));
            details.put("sourceKey", sourceKey);
        }
        
        if (destSeq != null) {
            details.put("destSequence", Long.parseLong(destSeq));
            details.put("destKey", destKey);
        }
        
        if (payload != null) {
            details.put("hasSourcePayload", true);
            details.put("sourcePayloadLength", payload.length());
        }
        
        if (destPayload != null) {
            details.put("hasDestPayload", true);
            details.put("destPayloadLength", destPayload.length());
        }
        
        if (sourceSeq == null && destSeq == null) {
            details.put("status", "not_found");
        } else {
            details.put("status", "found");
            if (sourceSeq != null && destSeq != null) {
                details.put("location", "both_source_and_dest");
            } else if (sourceSeq != null) {
                details.put("location", "source_only");
            } else {
                details.put("location", "dest_only");
            }
        }
        
        return details;
    }
}
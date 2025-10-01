package com.example.messagetransfer.controller;

import com.example.messagetransfer.service.RedisDebugService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Debug Controller for Redis inspection during development
 * Only enabled when debug.enabled=true in application properties
 */
@RestController
@RequestMapping("/debug")
@ConditionalOnProperty(name = "debug.enabled", havingValue = "true", matchIfMissing = true)
public class DebugController {

    private final RedisDebugService redisDebugService;

    public DebugController(RedisDebugService redisDebugService) {
        this.redisDebugService = redisDebugService;
    }

    /**
     * Get all Redis keys and values for debugging
     */
    @GetMapping("/redis/all")
    public ResponseEntity<?> getAllRedisKeys() {
        Map<String, Object> result = redisDebugService.getAllKeys();
        return ResponseEntity.ok(result);
    }

    /**
     * Get Redis statistics summary
     */
    @GetMapping("/redis/stats")
    public ResponseEntity<?> getRedisStats() {
        Map<String, Object> stats = redisDebugService.getStats();
        return ResponseEntity.ok(stats);
    }

    /**
     * Get specific order details from Redis
     */
    @GetMapping("/redis/order/{orderId}")
    public ResponseEntity<?> getOrderDetails(@PathVariable String orderId) {
        Map<String, Object> details = redisDebugService.getOrderDetails(orderId);
        return ResponseEntity.ok(details);
    }

    /**
     * Clear all order-related Redis keys (for testing)
     * BE CAREFUL: This will delete all order tracking data!
     */
    @DeleteMapping("/redis/clear-all")
    public ResponseEntity<?> clearAllRedisKeys() {
        Map<String, Object> result = redisDebugService.clearAll();
        return ResponseEntity.ok(result);
    }

    /**
     * Debug endpoint to check controller status
     */
    @GetMapping("/status")
    public ResponseEntity<?> debugStatus() {
        return ResponseEntity.ok(Map.of(
            "debugController", "active",
            "timestamp", java.time.Instant.now().toString(),
            "availableEndpoints", java.util.List.of(
                "GET /debug/redis/all - Get all Redis keys",
                "GET /debug/redis/stats - Get Redis statistics", 
                "GET /debug/redis/order/{orderId} - Get order details",
                "DELETE /debug/redis/clear-all - Clear all Redis data",
                "GET /debug/status - This endpoint"
            )
        ));
    }
}
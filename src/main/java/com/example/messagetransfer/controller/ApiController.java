package com.example.messagetransfer.controller;

import com.example.messagetransfer.dto.ScheduleRequest;
import com.example.messagetransfer.dto.ValidateResponse;
import com.example.messagetransfer.model.OrderPayload;
import com.example.messagetransfer.service.OrderGenerator;
import com.example.messagetransfer.service.ServiceBusSchedulerService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final OrderGenerator generator;
    private final ServiceBusSchedulerService service;

    @Value("${app.validation.peekCount:10}")
    private int defaultPeek;

    public ApiController(OrderGenerator generator, ServiceBusSchedulerService service) {
        this.generator = generator;
        this.service = service;
    }

    @PostMapping("/schedule")
    public ResponseEntity<?> schedule(@Valid @ModelAttribute ScheduleRequest req) {
        List<OrderPayload> orders = generator.generate(req.count());
        var results = service.scheduleOrders(orders, req.delaySeconds());
        return ResponseEntity.ok(results);
    }

    @PostMapping("/transfer")
    public ResponseEntity<?> transfer() {
        Map<String, Object> summary = service.transferAllFromRedis();
        return ResponseEntity.ok(summary);
    }

    @PostMapping("/transfer-enhanced")
    public ResponseEntity<?> transferEnhanced() {
        Map<String, Object> summary = service.transferMessagesWithRedisUpdate();
        return ResponseEntity.ok(summary);
    }

    @PostMapping("/cancel/{orderId}")
    public ResponseEntity<?> cancelOrder(@PathVariable String orderId) {
        Map<String, Object> result = service.cancelOrder(orderId);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<?> getOrderInfo(@PathVariable String orderId) {
        Map<String, Object> info = service.getOrderInfo(orderId);
        return ResponseEntity.ok(info);
    }

    @GetMapping("/health")
    public ResponseEntity<?> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "timestamp", java.time.Instant.now().toString(),
            "service", "Message Transfer Service"
        ));
    }

    @GetMapping("/validate")
    public ResponseEntity<?> validate(@RequestParam(name = "peek", required = false) Integer peek,
                                    @RequestParam(name = "includeTimings", required = false, defaultValue = "false") Boolean includeTimings) {
        int p = (peek == null) ? defaultPeek : Math.max(1, peek);
        
        if (includeTimings) {
            // Enhanced validation with timing analysis
            Map<String, Object> fullValidation = service.validateTransferWithQueuePeek(p);
            return ResponseEntity.ok(fullValidation);
        } else {
            // Simple queue peek validation
            var list = service.validatePeek(p);
            @SuppressWarnings("unchecked")
            ValidateResponse resp = new ValidateResponse(
                    (List<Map<String, Object>>) list.get(0).get("source"),
                    (List<Map<String, Object>>) list.get(1).get("destination")
            );
            return ResponseEntity.ok(resp);
        }
    }

    @DeleteMapping("/cleanup")
    public ResponseEntity<?> cleanupAllQueues() {
        Map<String, Object> result = service.cleanupAllQueues();
        return ResponseEntity.ok(result);
    }
}

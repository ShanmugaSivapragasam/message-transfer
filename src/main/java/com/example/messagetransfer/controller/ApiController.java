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

    @GetMapping("/validate")
    public ResponseEntity<?> validate(@RequestParam(name = "peek", required = false) Integer peek) {
        int p = (peek == null) ? defaultPeek : Math.max(1, peek);
        var list = service.validatePeek(p);
        ValidateResponse resp = new ValidateResponse(
                (List<Map<String, Object>>) list.get(0).get("source"),
                (List<Map<String, Object>>) list.get(1).get("destination")
        );
        return ResponseEntity.ok(resp);
    }
}

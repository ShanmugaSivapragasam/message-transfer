package com.example.messagetransfer.service;

import com.example.messagetransfer.model.*;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Component
public class OrderGenerator {

    private final Random random = new Random();

    public List<OrderPayload> generate(int count) {
        List<OrderPayload> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String orderId = "ORD-" + Instant.now().toString().substring(0,10) + "-" + String.format("%06d", random.nextInt(1_000_000));
            List<OrderLineItem> items = List.of(
                    new OrderLineItem("SKU-COF", "Coffee", 1, 3.50),
                    new OrderLineItem("SKU-FRY", "Fries", 1, 2.00),
                    new OrderLineItem("SKU-BUR", "Burger", 1, 6.50)
            );
            Payment payment = new Payment("CARD", "**** **** **** 4242", 12.00, "USD");
            Meta meta = new Meta("generic", "app", "1.0.0");
            list.add(new OrderPayload(orderId, Instant.now(), items, payment, meta));
        }
        return list;
    }
}

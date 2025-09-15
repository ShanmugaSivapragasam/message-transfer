package com.example.messagetransfer.model;

import java.time.Instant;
import java.util.List;

public record OrderPayload(
        String orderId,
        Instant placedAt,
        List<OrderLineItem> lineItems,
        Payment payment,
        Meta metadata
) {}

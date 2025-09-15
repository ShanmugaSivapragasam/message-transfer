package com.example.messagetransfer.model;

public record OrderLineItem(
        String sku,
        String name,
        int qty,
        double price
) {}

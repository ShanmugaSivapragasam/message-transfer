package com.example.messagetransfer.model;

public record Payment(
        String method,
        String maskedPan,
        double amount,
        String currency
) {}

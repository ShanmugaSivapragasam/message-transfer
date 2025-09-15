package com.example.messagetransfer.dto;

import jakarta.validation.constraints.Min;

public record ScheduleRequest(
        @Min(1) int count,
        @Min(0) int delaySeconds
) {}

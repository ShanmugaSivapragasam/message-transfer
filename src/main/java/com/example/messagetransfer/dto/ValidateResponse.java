package com.example.messagetransfer.dto;

import java.util.List;
import java.util.Map;

public record ValidateResponse(
        List<Map<String, Object>> sourcePeek,
        List<Map<String, Object>> destPeek
) {}

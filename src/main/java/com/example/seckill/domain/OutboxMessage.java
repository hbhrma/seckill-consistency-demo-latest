package com.example.seckill.domain;

import java.time.LocalDateTime;

public record OutboxMessage(
        long id,
        String eventId,
        String bizKey,
        OutboxEventType eventType,
        String payload,
        String status,
        int retryCount,
        LocalDateTime nextRetryAt,
        String lastError,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}

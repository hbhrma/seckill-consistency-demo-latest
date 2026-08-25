package com.example.seckill.domain;

import java.time.LocalDateTime;

public record CompensationTask(
        long id,
        String bizKey,
        String taskType,
        String orderNo,
        long userId,
        long goodsId,
        String reason,
        String status,
        int retryCount,
        LocalDateTime nextRetryAt,
        String lastError
) {
}

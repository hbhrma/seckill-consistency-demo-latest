package com.example.seckill.domain;

import java.time.LocalDateTime;

public record SeckillOrder(
        long id,
        String orderNo,
        long userId,
        long goodsId,
        OrderStatus status,
        LocalDateTime expireTime,
        LocalDateTime paidAt,
        LocalDateTime canceledAt,
        boolean stockReleased,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}

package com.example.seckill.dto;

public record SeckillResponse(
        boolean accepted,
        String orderNo,
        String message
) {
}

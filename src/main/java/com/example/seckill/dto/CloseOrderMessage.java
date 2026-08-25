package com.example.seckill.dto;

public record CloseOrderMessage(
        String orderNo,
        long expireAtEpochMillis
) {
}

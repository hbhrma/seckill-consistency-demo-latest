package com.example.seckill.dto;

public record CreateOrderMessage(
        String orderNo,
        long userId,
        long goodsId
) {
}

package com.example.seckill.dto;

public record ReleaseStockMessage(
        String orderNo,
        long userId,
        long goodsId
) {
}

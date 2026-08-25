package com.example.seckill.domain;

public record Reservation(
        String orderNo,
        long userId,
        long goodsId,
        ReservationState state,
        long createdAtMillis,
        long leaseUntilMillis
) {
}

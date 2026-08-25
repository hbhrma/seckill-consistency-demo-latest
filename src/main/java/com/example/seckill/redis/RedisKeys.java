package com.example.seckill.redis;

public final class RedisKeys {

    private RedisKeys() {
    }

    /** 对账任务使用：score 表示下一次允许对账/检查的时间。 */
    public static final String PENDING_RESERVATION_ZSET = "seckill:reservation:pending";

    /** Producer 可靠发送 CREATE_ORDER 使用：score 表示下一次允许重试发送的时间。 */
    public static final String CREATE_ORDER_SEND_PENDING_ZSET = "seckill:create-order:send-pending";

    public static String stock(long goodsId) {
        return "seckill:stock:" + goodsId;
    }

    public static String buyers(long goodsId) {
        return "seckill:buyers:" + goodsId;
    }

    public static String reservation(String orderNo) {
        return "seckill:reservation:" + orderNo;
    }
}

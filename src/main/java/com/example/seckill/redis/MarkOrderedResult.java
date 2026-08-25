package com.example.seckill.redis;

/**
 * Redis reservation 在执行 markOrdered 时的结果。
 *
 * 正常状态机：
 * RESERVED -> CREATING -> ORDERED -> RELEASED
 *
 * markOrdered 只允许把 CREATING 推进到 ORDERED；
 * ORDERED 幂等成功；
 * RELEASED 是合法终态，但不能反向改成 ORDERED，需要 Java 层重新查询 DB 最新订单状态；
 * RESERVED 在当前协议下属于非法状态。
 */
public enum MarkOrderedResult {
    MARKED_ORDERED,
    ALREADY_ORDERED,
    ALREADY_RELEASED,
    RESERVED_INVALID,
    MISSING,
    INVALID_STATE,
    UNKNOWN
}

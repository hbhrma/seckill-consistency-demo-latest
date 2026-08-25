package com.example.seckill.redis;

public enum ReleaseResult {
    RELEASED_NOW,
    ALREADY_RELEASED,
    ORDERED_NOT_ALLOWED,
    INVALID_STATE,
    MISSING,
    UNKNOWN
}

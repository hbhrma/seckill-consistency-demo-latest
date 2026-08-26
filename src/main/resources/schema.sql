CREATE DATABASE IF NOT EXISTS seckill_demo
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE seckill_demo;

CREATE TABLE IF NOT EXISTS seckill_order (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_no VARCHAR(64) NOT NULL,
    user_id BIGINT NOT NULL,
    goods_id BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    expire_time DATETIME(3) NOT NULL,
    paid_at DATETIME(3) NULL,
    canceled_at DATETIME(3) NULL,
    stock_released TINYINT NOT NULL DEFAULT 0,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    UNIQUE KEY uk_order_no(order_no),
    KEY idx_status_expire(status, expire_time),
    KEY idx_cancel_release(status, stock_released),
    KEY idx_user_goods(user_id, goods_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS seckill_order_create_guard (
    order_no VARCHAR(64) PRIMARY KEY,
    state VARCHAR(16) NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    KEY idx_guard_state(state)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS outbox_message (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id VARCHAR(64) NOT NULL,
    biz_key VARCHAR(128) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(16) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_at DATETIME(3) NOT NULL,
    last_error VARCHAR(1000) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    UNIQUE KEY uk_event_id(event_id),
    UNIQUE KEY uk_biz_key(biz_key),
    KEY idx_outbox_scan(status, next_retry_at),
    KEY idx_outbox_sending(status, updated_at)
) ENGINE=InnoDB;

package com.example.seckill.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderCreateGuardRepository {

    public static final String CREATING = "CREATING";
    public static final String CREATED = "CREATED";
    public static final String ABORTED = "ABORTED";

    private final JdbcTemplate jdbcTemplate;

    public OrderCreateGuardRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 必须在 @Transactional 方法中调用。
     *
     * 语义：
     * 1. 如果 guard 不存在，则以 initialState 创建；
     * 2. 如果 guard 已存在，则做一次 no-op update。
     *
     * InnoDB 会对唯一键 order_no 对应的记录加排他锁。
     * 因此，如果另一个事务正在以同一个 orderNo 创建订单，
     * 本事务会等它提交/回滚之后再继续，从而把“创建”和“终止创建”串行化。
     */
    public String lockOrCreate(String orderNo, String initialState) {
        jdbcTemplate.update("""
                INSERT INTO seckill_order_create_guard(order_no, state)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE order_no = order_no
                """, orderNo, initialState);

        return jdbcTemplate.queryForObject("""
                SELECT state
                FROM seckill_order_create_guard
                WHERE order_no = ?
                FOR UPDATE
                """, String.class, orderNo);
    }

    public int markCreated(String orderNo) {
        return jdbcTemplate.update("""
                UPDATE seckill_order_create_guard
                SET state = 'CREATED',
                    updated_at = NOW(3)
                WHERE order_no = ?
                  AND state = 'CREATING'
                """, orderNo);
    }
}

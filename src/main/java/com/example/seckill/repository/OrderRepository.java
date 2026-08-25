package com.example.seckill.repository;

import com.example.seckill.domain.OrderStatus;
import com.example.seckill.domain.SeckillOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public class OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    private final RowMapper<SeckillOrder> rowMapper = (rs, rowNum) -> new SeckillOrder(
            rs.getLong("id"),
            rs.getString("order_no"),
            rs.getLong("user_id"),
            rs.getLong("goods_id"),
            OrderStatus.valueOf(rs.getString("status")),
            rs.getTimestamp("expire_time").toLocalDateTime(),
            toLocalDateTime(rs.getTimestamp("paid_at")),
            toLocalDateTime(rs.getTimestamp("canceled_at")),
            rs.getBoolean("stock_released"),
            rs.getTimestamp("created_at").toLocalDateTime(),
            rs.getTimestamp("updated_at").toLocalDateTime()
    );

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<SeckillOrder> findByOrderNo(String orderNo) {
        List<SeckillOrder> list = jdbcTemplate.query("""
                SELECT * FROM seckill_order WHERE order_no = ?
                """, rowMapper, orderNo);
        return list.stream().findFirst();
    }

    public void insertWaitingOrder(String orderNo,
                                   long userId,
                                   long goodsId,
                                   LocalDateTime expireTime) {
        jdbcTemplate.update("""
                INSERT INTO seckill_order
                    (order_no, user_id, goods_id, status, expire_time, stock_released)
                VALUES (?, ?, ?, 'WAIT_PAY', ?, 0)
                """,
                orderNo,
                userId,
                goodsId,
                Timestamp.valueOf(expireTime)
        );
    }

    public int tryPay(String orderNo) {
        return jdbcTemplate.update("""
                UPDATE seckill_order
                SET status = 'PAID',
                    paid_at = NOW(3),
                    updated_at = NOW(3)
                WHERE order_no = ?
                  AND status = 'WAIT_PAY'
                """, orderNo);
    }

    public int tryCloseExpired(String orderNo) {
        return jdbcTemplate.update("""
                UPDATE seckill_order
                SET status = 'CANCELED',
                    canceled_at = NOW(3),
                    updated_at = NOW(3)
                WHERE order_no = ?
                  AND status = 'WAIT_PAY'
                  AND expire_time <= NOW(3)
                """, orderNo);
    }

    public int markStockReleased(String orderNo) {
        return jdbcTemplate.update("""
                UPDATE seckill_order
                SET stock_released = 1,
                    updated_at = NOW(3)
                WHERE order_no = ?
                  AND status = 'CANCELED'
                  AND stock_released = 0
                """, orderNo);
    }

    public List<String> findExpiredWaitingOrderNos(int limit) {
        return jdbcTemplate.query("""
                SELECT order_no
                FROM seckill_order
                WHERE status = 'WAIT_PAY'
                  AND expire_time <= NOW(3)
                ORDER BY expire_time
                LIMIT ?
                """, (rs, rowNum) -> rs.getString(1), limit);
    }

    public List<String> findCanceledNotReleasedOrderNos(int limit) {
        return jdbcTemplate.query("""
                SELECT order_no
                FROM seckill_order
                WHERE status = 'CANCELED'
                  AND stock_released = 0
                ORDER BY updated_at
                LIMIT ?
                """, (rs, rowNum) -> rs.getString(1), limit);
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}

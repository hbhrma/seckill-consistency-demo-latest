package com.example.seckill.repository;

import com.example.seckill.domain.OutboxEventType;
import com.example.seckill.domain.OutboxMessage;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    private final RowMapper<OutboxMessage> rowMapper = (rs, rowNum) -> new OutboxMessage(
            rs.getLong("id"),
            rs.getString("event_id"),
            rs.getString("biz_key"),
            OutboxEventType.valueOf(rs.getString("event_type")),
            rs.getString("payload"),
            rs.getString("status"),
            rs.getInt("retry_count"),
            rs.getTimestamp("next_retry_at").toLocalDateTime(),
            rs.getString("last_error"),
            rs.getTimestamp("created_at").toLocalDateTime(),
            rs.getTimestamp("updated_at").toLocalDateTime()
    );

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(String eventId,
                       String bizKey,
                       OutboxEventType type,
                       String payload,
                       LocalDateTime nextRetryAt) {
        jdbcTemplate.update("""
                INSERT INTO outbox_message
                    (event_id, biz_key, event_type, payload, status, retry_count, next_retry_at)
                VALUES (?, ?, ?, ?, 'NEW', 0, ?)
                """,
                eventId,
                bizKey,
                type.name(),
                payload,
                Timestamp.valueOf(nextRetryAt)
        );
    }

    /**
     * 异常/历史数据修复路径使用。biz_key 有唯一索引，因此该操作天然幂等。
     * 已存在时做 no-op，不覆盖原消息 payload/状态。
     */
    public int insertIfAbsent(String eventId,
                              String bizKey,
                              OutboxEventType type,
                              String payload,
                              LocalDateTime nextRetryAt) {
        return jdbcTemplate.update("""
                INSERT INTO outbox_message
                    (event_id, biz_key, event_type, payload, status, retry_count, next_retry_at)
                VALUES (?, ?, ?, ?, 'NEW', 0, ?)
                ON DUPLICATE KEY UPDATE biz_key = biz_key
                """,
                eventId,
                bizKey,
                type.name(),
                payload,
                Timestamp.valueOf(nextRetryAt)
        );
    }

    public List<OutboxMessage> findDue(int limit) {
        return jdbcTemplate.query("""
                SELECT *
                FROM outbox_message
                WHERE status IN ('NEW', 'RETRY')
                  AND next_retry_at <= NOW(3)
                ORDER BY id
                LIMIT ?
                """, rowMapper, limit);
    }

    public int tryClaim(long id) {
        return jdbcTemplate.update("""
                UPDATE outbox_message
                SET status = 'SENDING',
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status IN ('NEW', 'RETRY')
                  AND next_retry_at <= NOW(3)
                """, id);
    }

    public void markSent(long id) {
        jdbcTemplate.update("""
                UPDATE outbox_message
                SET status = 'SENT',
                    last_error = NULL,
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'SENDING'
                """, id);
    }

    public void markRetry(long id,
                          int nextRetryCount,
                          boolean dead,
                          LocalDateTime nextRetryAt,
                          String error) {
        jdbcTemplate.update("""
                UPDATE outbox_message
                SET status = ?,
                    retry_count = ?,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'SENDING'
                """,
                dead ? "DEAD" : "RETRY",
                nextRetryCount,
                Timestamp.valueOf(nextRetryAt),
                trim(error),
                id
        );
    }


    /**
     * 扫描已经进入 DEAD、且已经到达下一次业务补偿检查时间的 Outbox。
     * DEAD 不再由普通 Publisher 盲目重发，而是交给业务级补偿逻辑重新查订单事实。
     */
    public List<OutboxMessage> findDueDead(int limit) {
        return jdbcTemplate.query("""
                SELECT *
                FROM outbox_message
                WHERE status = 'DEAD'
                  AND next_retry_at <= NOW(3)
                ORDER BY next_retry_at, id
                LIMIT ?
                """, rowMapper, limit);
    }

    /**
     * CLOSE_ORDER=DEAD，但订单仍是未过期 WAIT_PAY 时，恢复到普通发送链路。
     * retry_count 故意不清零：保留历史失败次数；如果这次再次失败，可以重新进入 DEAD，
     * 再由 DEAD handler 重新判断最新订单状态。
     */
    public int retryDeadNow(long id) {
        return jdbcTemplate.update("""
                UPDATE outbox_message
                SET status = 'RETRY',
                    next_retry_at = NOW(3),
                    last_error = CONCAT(COALESCE(last_error, ''), ' | DEAD recovered to RETRY'),
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'DEAD'
                """, id);
    }

    /**
     * DEAD 消息已经通过业务级补偿完成闭环。
     * 例如 CLOSE_ORDER 对应订单已经 PAID；或者库存已经确认释放完成。
     */
    public int resolveDead(long id, String reason) {
        return jdbcTemplate.update("""
                UPDATE outbox_message
                SET status = 'RESOLVED',
                    last_error = ?,
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'DEAD'
                """,
                trim(reason),
                id
        );
    }

    /**
     * DEAD 补偿本身也失败时，不改变 DEAD，只把下一次业务补偿检查时间往后推。
     */
    public int rescheduleDeadCheck(long id,
                                   LocalDateTime nextCheckAt,
                                   String error) {
        return jdbcTemplate.update("""
                UPDATE outbox_message
                SET next_retry_at = ?,
                    last_error = ?,
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'DEAD'
                """,
                Timestamp.valueOf(nextCheckAt),
                trim(error),
                id
        );
    }

    public int recoverStuckSending(long timeoutSeconds) {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(timeoutSeconds);
        return jdbcTemplate.update("""
                UPDATE outbox_message
                SET status = 'RETRY',
                    next_retry_at = NOW(3),
                    last_error = CONCAT(COALESCE(last_error, ''), ' | recovered stuck SENDING'),
                    updated_at = NOW(3)
                WHERE status = 'SENDING'
                  AND updated_at < ?
                """, Timestamp.valueOf(cutoff));
    }

    private static String trim(String error) {
        if (error == null) return null;
        return error.length() <= 1000 ? error : error.substring(0, 1000);
    }
}

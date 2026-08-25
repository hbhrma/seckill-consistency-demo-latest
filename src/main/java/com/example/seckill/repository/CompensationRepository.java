package com.example.seckill.repository;

import com.example.seckill.domain.CompensationTask;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public class CompensationRepository {

    private final JdbcTemplate jdbcTemplate;

    private final RowMapper<CompensationTask> rowMapper = (rs, rowNum) -> new CompensationTask(
            rs.getLong("id"),
            rs.getString("biz_key"),
            rs.getString("task_type"),
            rs.getString("order_no"),
            rs.getLong("user_id"),
            rs.getLong("goods_id"),
            rs.getString("reason"),
            rs.getString("status"),
            rs.getInt("retry_count"),
            rs.getTimestamp("next_retry_at").toLocalDateTime(),
            rs.getString("last_error")
    );

    public CompensationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertIfAbsent(String orderNo,
                               long userId,
                               long goodsId,
                               String reason) {
        jdbcTemplate.update("""
                INSERT IGNORE INTO compensation_task
                    (biz_key, task_type, order_no, user_id, goods_id, reason,
                     status, retry_count, next_retry_at)
                VALUES (?, 'RELEASE_UNCREATED_ORDER', ?, ?, ?, ?, 'PENDING', 0, NOW(3))
                """,
                "RELEASE_UNCREATED_ORDER:" + orderNo,
                orderNo,
                userId,
                goodsId,
                trim(reason)
        );
    }

    public List<CompensationTask> findDue(int limit) {
        return jdbcTemplate.query("""
                SELECT *
                FROM compensation_task
                WHERE status IN ('PENDING', 'RETRY')
                  AND next_retry_at <= NOW(3)
                ORDER BY id
                LIMIT ?
                """, rowMapper, limit);
    }

    public int tryClaim(long id) {
        return jdbcTemplate.update("""
                UPDATE compensation_task
                SET status = 'PROCESSING',
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status IN ('PENDING', 'RETRY')
                  AND next_retry_at <= NOW(3)
                """, id);
    }

    public void markDone(long id, String note) {
        jdbcTemplate.update("""
                UPDATE compensation_task
                SET status = 'DONE',
                    last_error = ?,
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'PROCESSING'
                """, trim(note), id);
    }

    public void markRetry(long id,
                          int nextRetryCount,
                          boolean dead,
                          LocalDateTime nextRetryAt,
                          String error) {
        jdbcTemplate.update("""
                UPDATE compensation_task
                SET status = ?,
                    retry_count = ?,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = NOW(3)
                WHERE id = ?
                  AND status = 'PROCESSING'
                """,
                dead ? "DEAD" : "RETRY",
                nextRetryCount,
                Timestamp.valueOf(nextRetryAt),
                trim(error),
                id
        );
    }


    public int recoverStuckProcessing(long timeoutSeconds) {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(timeoutSeconds);
        return jdbcTemplate.update("""
                UPDATE compensation_task
                SET status = 'RETRY',
                    next_retry_at = NOW(3),
                    last_error = CONCAT(COALESCE(last_error, ''), ' | recovered stuck PROCESSING'),
                    updated_at = NOW(3)
                WHERE status = 'PROCESSING'
                  AND updated_at < ?
                """, Timestamp.valueOf(cutoff));
    }

    private static String trim(String error) {
        if (error == null) return null;
        return error.length() <= 1000 ? error : error.substring(0, 1000);
    }
}

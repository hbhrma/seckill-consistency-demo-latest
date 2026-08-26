package com.example.seckill.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "seckill")
public class SeckillProperties {

    private long paymentTimeoutSeconds = 1800;
    private long creatingLeaseSeconds = 60;
    private long reconcileGraceSeconds = 60;                // 600太长了
    private long reservationRetentionSeconds = 604800;
    private Scheduler scheduler = new Scheduler();
    private Mq mq = new Mq();

    public long getPaymentTimeoutSeconds() {
        return paymentTimeoutSeconds;
    }

    public void setPaymentTimeoutSeconds(long paymentTimeoutSeconds) {
        this.paymentTimeoutSeconds = paymentTimeoutSeconds;
    }

    public long getCreatingLeaseSeconds() {
        return creatingLeaseSeconds;
    }

    public void setCreatingLeaseSeconds(long creatingLeaseSeconds) {
        this.creatingLeaseSeconds = creatingLeaseSeconds;
    }

    public long getReconcileGraceSeconds() {
        return reconcileGraceSeconds;
    }

    public void setReconcileGraceSeconds(long reconcileGraceSeconds) {
        this.reconcileGraceSeconds = reconcileGraceSeconds;
    }

    public long getReservationRetentionSeconds() {
        return reservationRetentionSeconds;
    }

    public void setReservationRetentionSeconds(long reservationRetentionSeconds) {
        this.reservationRetentionSeconds = reservationRetentionSeconds;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Mq getMq() {
        return mq;
    }

    public void setMq(Mq mq) {
        this.mq = mq;
    }

    public static class Scheduler {
        private long fixedDelayMs = 3000;
        private int batchSize = 100;
        private int outboxMaxRetry = 10;
        private long retryBaseSeconds = 5;
        private long sendingTimeoutSeconds = 120;

        public long getFixedDelayMs() {
            return fixedDelayMs;
        }

        public void setFixedDelayMs(long fixedDelayMs) {
            this.fixedDelayMs = fixedDelayMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getOutboxMaxRetry() {
            return outboxMaxRetry;
        }

        public void setOutboxMaxRetry(int outboxMaxRetry) {
            this.outboxMaxRetry = outboxMaxRetry;
        }

        public long getRetryBaseSeconds() {
            return retryBaseSeconds;
        }

        public void setRetryBaseSeconds(long retryBaseSeconds) {
            this.retryBaseSeconds = retryBaseSeconds;
        }

        public long getSendingTimeoutSeconds() {
            return sendingTimeoutSeconds;
        }

        public void setSendingTimeoutSeconds(long sendingTimeoutSeconds) {
            this.sendingTimeoutSeconds = sendingTimeoutSeconds;
        }
    }

    public static class Mq {
        private String createOrderTopic;
        private String closeOrderTopic;
        private String releaseStockTopic;
        private String createOrderConsumerGroup;
        private String closeOrderConsumerGroup;
        private String releaseStockConsumerGroup;

        public String getCreateOrderTopic() {
            return createOrderTopic;
        }

        public void setCreateOrderTopic(String createOrderTopic) {
            this.createOrderTopic = createOrderTopic;
        }

        public String getCloseOrderTopic() {
            return closeOrderTopic;
        }

        public void setCloseOrderTopic(String closeOrderTopic) {
            this.closeOrderTopic = closeOrderTopic;
        }

        public String getReleaseStockTopic() {
            return releaseStockTopic;
        }

        public void setReleaseStockTopic(String releaseStockTopic) {
            this.releaseStockTopic = releaseStockTopic;
        }

        public String getCreateOrderConsumerGroup() {
            return createOrderConsumerGroup;
        }

        public void setCreateOrderConsumerGroup(String createOrderConsumerGroup) {
            this.createOrderConsumerGroup = createOrderConsumerGroup;
        }

        public String getCloseOrderConsumerGroup() {
            return closeOrderConsumerGroup;
        }

        public void setCloseOrderConsumerGroup(String closeOrderConsumerGroup) {
            this.closeOrderConsumerGroup = closeOrderConsumerGroup;
        }

        public String getReleaseStockConsumerGroup() {
            return releaseStockConsumerGroup;
        }

        public void setReleaseStockConsumerGroup(String releaseStockConsumerGroup) {
            this.releaseStockConsumerGroup = releaseStockConsumerGroup;
        }
    }
}

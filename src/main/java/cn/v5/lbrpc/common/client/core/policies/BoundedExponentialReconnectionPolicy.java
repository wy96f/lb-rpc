package cn.v5.lbrpc.common.client.core.policies;

/**
 * Created by yangwei on 15-5-12.
 */
public class BoundedExponentialReconnectionPolicy implements ReconnectionPolicy {
    private final long baseInterval;
    private final long maxInterval;
    private final int maxRetryCount;

    /**
     *
     * @param baseInterval ms
     * @param maxInterval  ms
     * @param maxRetryCount
     */
    public BoundedExponentialReconnectionPolicy(long baseInterval, long maxInterval, int maxRetryCount) {
        this.baseInterval = baseInterval;
        this.maxInterval = maxInterval;
        this.maxRetryCount = maxRetryCount;
    }

    @Override
    public boolean retry(Exception e, int retryCount, long elapsedTime) {
        return retryCount < maxRetryCount;
    }

    @Override
    public ReconnectionSchedule newSchedule() {
        return new ExponentialSchedule();
    }

    private class ExponentialSchedule implements ReconnectionSchedule {
        private int attempt = 0;
        private long elapsedTime = 0;

        @Override
        public long nextDelayMs() {
            long delay = Math.min(maxInterval, baseInterval * (1 << attempt++));
            elapsedTime += delay;
            return delay;
        }

        @Override
        public int retryCount() {
            return attempt;
        }

        @Override
        public long elapsedTime() {
            return elapsedTime;
        }
    }
}

package cn.v5.lbrpc.common.client.core.policies;

/**
 * Created by yangwei on 15-5-12.
 */
public interface ReconnectionPolicy {
    public ReconnectionSchedule newSchedule();

    public boolean retry(Exception e, int retryCount, long elapsedTime);

    public interface ReconnectionSchedule {
        public long nextDelayMs();
        public int retryCount();
        public long elapsedTime();
    }
}

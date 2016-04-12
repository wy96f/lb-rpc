package cn.v5.lbrpc.common.client.core;

import com.google.common.base.Preconditions;

/**
 * Created by yangwei on 15-5-26.
 */
public class PoolOptions {
    private static final int DEFAULT_POOL_TIMEOUT_MILLIS = 5000;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;

    private static final int DEFAULT_MAX_POOL = 4;

    private int poolTimeoutMs = DEFAULT_POOL_TIMEOUT_MILLIS;
    private int heartbeatIntervalSec = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
    private int maxPool = DEFAULT_MAX_POOL;

    public int getMaxPool() {
        return maxPool;
    }

    public PoolOptions setMaxPool(int maxPool) {
        Preconditions.checkArgument(poolTimeoutMs > 0, "max pool num must be positive");
        this.maxPool = maxPool;
        return this;
    }

    public int getPoolTimeoutMs() {
        return poolTimeoutMs;
    }

    public PoolOptions setPoolTimeoutMs(int poolTimeoutMs) {
        Preconditions.checkArgument(poolTimeoutMs > 0, "pool timeout must be positive");
        this.poolTimeoutMs = poolTimeoutMs;
        return this;
    }

    public int getHeartbeatIntervalSec() {
        return heartbeatIntervalSec;
    }

    public PoolOptions setHeartbeatIntervalSec(int heartbeatIntervalSec) {
        Preconditions.checkArgument(heartbeatIntervalSec > 0, "heart beat interval must be positive");
        this.heartbeatIntervalSec = heartbeatIntervalSec;
        return this;
    }
}

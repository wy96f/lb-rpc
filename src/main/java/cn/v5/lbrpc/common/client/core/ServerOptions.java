package cn.v5.lbrpc.common.client.core;

/**
 * Created by yangwei on 15-5-27.
 */
public class ServerOptions {
    private static final int DEFAULT_MAX_THREADS = 16;

    private int max_threads = DEFAULT_MAX_THREADS;

    public int getMax_threads() {
        return max_threads;
    }

    public ServerOptions setMax_threads(int max_threads) {
        this.max_threads = max_threads;
        return this;
    }
}

package cn.v5.lbrpc.common.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangwei on 15-5-6.
 */
public class StreamIdGenerator {
    private final AtomicInteger streamId;

    public StreamIdGenerator() {
        this.streamId = new AtomicInteger(0);
    }

    public static StreamIdGenerator newInstance() {
        return new StreamIdGenerator();
    }

    public int next() {
        if (streamId.get() > Integer.MAX_VALUE - 10000) {
            return streamId.getAndSet(0);
        }
        return streamId.addAndGet(1);
    }
}
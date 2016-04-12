package cn.v5.lbrpc.common.client.core;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-5-4.
 */
public interface ResultFuture<T> extends ListenableFuture<T> {
    public T getUninterruptibly();

    public T getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning);
}

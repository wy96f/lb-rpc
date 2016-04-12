package cn.v5.lbrpc.common.utils;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangwei on 15-5-5.
 */
public class ExecutorUtil {
    private static final int DEFAULT_THREAD_KEEP_ALIVE = 30;


    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    public static ListeningExecutorService makeExecutor(int threads, String name, LinkedBlockingQueue<Runnable> workQueue) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads,
                threads,
                DEFAULT_THREAD_KEEP_ALIVE,
                TimeUnit.SECONDS,
                workQueue,
                threadFactory(name));

        executor.allowCoreThreadTimeOut(true);
        return MoreExecutors.listeningDecorator(executor);
    }
}

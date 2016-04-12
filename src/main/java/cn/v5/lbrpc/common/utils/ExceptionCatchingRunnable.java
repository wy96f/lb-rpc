package cn.v5.lbrpc.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangwei on 15-5-6.
 */
public abstract class ExceptionCatchingRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionCatchingRunnable.class);

    public abstract void runMayThrow() throws Exception;

    @Override
    public void run() {
        try {
            runMayThrow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (Exception e) {
            logger.error("Unexpected error while executing task", e);
        }
    }
}

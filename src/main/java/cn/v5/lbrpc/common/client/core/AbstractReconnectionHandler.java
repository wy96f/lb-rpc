package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.client.core.policies.ReconnectionPolicy;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yangwei on 15-5-12.
 */
public abstract class AbstractReconnectionHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractReconnectionHandler.class);

    private final ScheduledExecutorService executor;
    private final ReconnectionPolicy.ReconnectionSchedule schedule;

    private final long initDelayMs;

    private final AtomicReference<ListenableFuture<?>> currentAttempt;

    private final HandlerFuture handlerFuture = new HandlerFuture();

    private final CountDownLatch ready = new CountDownLatch(1);

    protected AbstractReconnectionHandler(ScheduledExecutorService executor, ReconnectionPolicy.ReconnectionSchedule schedule, AtomicReference<ListenableFuture<?>> currentAttempt) {
        this(executor, schedule, -1, currentAttempt);
    }

    protected AbstractReconnectionHandler(ScheduledExecutorService executor, ReconnectionPolicy.ReconnectionSchedule schedule, long initDelayMs, AtomicReference<ListenableFuture<?>> currentAttempt) {
        this.executor = executor;
        this.schedule = schedule;
        this.initDelayMs = initDelayMs;
        this.currentAttempt = currentAttempt;
    }

    public abstract IPrimeConnection tryReconnect() throws ConnectionException, InterruptedException;

    public abstract void onReconnection(IPrimeConnection connection);

    public void start() {
        long firstDelay = initDelayMs >= 0 ? initDelayMs : schedule.nextDelayMs();
        logger.debug("First reconnection scheduled in {}ms", firstDelay);

        try {
            handlerFuture.nextTry = executor.schedule(this, firstDelay, TimeUnit.MILLISECONDS);

            while (true) {
                ListenableFuture<?> previous = currentAttempt.get();
                if (previous != null && !previous.isCancelled()) {
                    logger.debug("Found annother already active handler, cancelling");
                    handlerFuture.cancel(false);
                    break;
                }

                if (currentAttempt.compareAndSet(previous, handlerFuture)) {
                    logger.debug("Becoming the active handler");
                    break;
                }
            }
            ready.countDown();
        } catch (RejectedExecutionException e) {
            logger.debug("Aborting reconnection handling since the manager is shutting down");
        }
    }

    public boolean onConnectionException(ConnectionException e, int retryCount, long elapsedTime, long nextDelayMs) {
        return true;
    }

    public boolean onUnknownException(Exception e, int retryCount, long elapsedTime, long nextDelayMs) {
        return true;
    }

    @Override
    public void run() {
        try {
            ready.await();
        } catch (InterruptedException e) {
            // This can happen at shutdown
            Thread.currentThread().interrupt();
            return;
        }

        if (handlerFuture.isCancelled()) {
            logger.debug("Got cancelled, stopping");
            currentAttempt.compareAndSet(handlerFuture, null);
            return;
        }

        try {
            onReconnection(tryReconnect());
            handlerFuture.markDone();
            currentAttempt.compareAndSet(handlerFuture, null);
            logger.debug("Reconnection successful, cleared the future");
        } catch (ConnectionException e) {
            int retryCount = schedule.retryCount();
            long elapsedTime = schedule.elapsedTime();
            long nextDelay = schedule.nextDelayMs();
            if (onConnectionException(e, retryCount, elapsedTime, nextDelay))
                reschedule(nextDelay);
            else {
                currentAttempt.compareAndSet(handlerFuture, null);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (Exception e) {
            int retryCount = schedule.retryCount();
            long elapsedTime = schedule.elapsedTime();
            long nextDelay = schedule.nextDelayMs();
            if (onUnknownException(e, retryCount, elapsedTime, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(handlerFuture, null);
        }
    }

    private void reschedule(long nextDelayMs) {
        // If we got cancelled during the failed reconnection attempt that lead here, don't reschedule
        if (handlerFuture.isCancelled()) {
            currentAttempt.compareAndSet(handlerFuture, null);
            return;
        }
        handlerFuture.nextTry = executor.schedule(this, nextDelayMs, TimeUnit.MILLISECONDS);
    }

    private class HandlerFuture extends AbstractFuture {
        private ScheduledFuture nextTry;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // This is a check-then-act, so we may race with the scheduling of the first try, but in that case
            // we'll re-check for cancellation when this first try starts ru
            if (this.nextTry != null) {
                nextTry.cancel(mayInterruptIfRunning);
            }
            return super.cancel(mayInterruptIfRunning);
        }

        public void markDone() {
            super.set(null);
        }
    }
}

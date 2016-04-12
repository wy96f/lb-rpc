package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.client.core.policies.ReconnectionPolicy;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.ExceptionCatchingRunnable;
import cn.v5.lbrpc.common.utils.ExecutorUtil;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

/**
 * Created by yangwei on 15-6-8.
 */
public abstract class AbstractManager<T extends IRequest, V extends IResponse> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractManager.class);

    public final ListeningExecutorService executor;
    public final ListeningExecutorService blockingExecutor;
    public final ScheduledThreadPoolExecutor reconnectionExecutor;
    public final Configuration configuration;
    public final Striped<Lock> poolCreationLocks = Striped.lazyWeakLock(5);
    final LinkedBlockingQueue<Runnable> executorQueue = new LinkedBlockingQueue<Runnable>();
    final LinkedBlockingQueue<Runnable> blockingExecutorQueue = new LinkedBlockingQueue<Runnable>();
    final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();

    public volatile boolean closed = false;

    public volatile boolean isClosing;

    public AbstractManager(Configuration configuration) {
        this.configuration = configuration;
        this.executor = ExecutorUtil.makeExecutor(2, getPrefix() + " Proxy worker-%d", executorQueue);
        this.blockingExecutor = ExecutorUtil.makeExecutor(2, getPrefix() + " Proxy blocking tasks worker-%d", blockingExecutorQueue);
        this.reconnectionExecutor = new ScheduledThreadPoolExecutor(2, threadFactory(getPrefix() + " Reconnection-%d"));
    }

    static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    public synchronized void init(Pair<String, String> serviceAndProto, List<InetSocketAddress> initContactPoints) {
        if (initContactPoints.size() == 0) {
            throw new NoHostAvailableException(new HashMap<InetSocketAddress, Throwable>());
        }

        List<Host> initPoolHosts = Lists.newArrayList();
        List<Host> allPoolHosts = Lists.newArrayList();
        for (InetSocketAddress address : initContactPoints) {
            Host host = addHost(serviceAndProto, address);
            if (host != null) {
                // The new host can be added here and in AbstractNodeClient.serverListChanged, only one thread succeeds
                // host.setUp();
                initPoolHosts.add(host);
                host.setUp();
            }
            allPoolHosts.add(getHost(address));
        }

        getLoadBalancingPolicy(serviceAndProto).init(allPoolHosts);
        // make sure we only init and create new host pools, don't touch those initialized
        createPools(initPoolHosts);
    }

    private void createPools(Collection<Host> hosts) {
        List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(hosts.size());
        for (Host host : hosts) {
            if (host.state != Host.State.DOWN)
                futures.add(maybeAddPool(host, executor));
        }
        ListenableFuture<List<Boolean>> f = Futures.allAsList(futures);
        try {
            f.get();
        } catch (ExecutionException e) {
            throw new RpcInternalError(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
    }

    public Host getHost(InetSocketAddress address) {
        return hosts.get(address);
    }

    public Host addHost(Pair<String, String> serviceAndProto, InetSocketAddress address) {
        Host newHost = new Host(serviceAndProto, address);
        Host previous = hosts.putIfAbsent(address, newHost);
        if (previous == null) {
            newHost.addService(serviceAndProto);
        } else {
            previous.addService(serviceAndProto);
        }
        return previous == null ? newHost : null;
    }

    public ReconnectionPolicy reconnectionPolicy() {
        return configuration.getReconnectionPolicy();
    }

    public void addLoadBalancingPolicy(Pair<String, String> serviceAndProto, LoadBalancingPolicy loadBalancingPolicy) {
        this.configuration.addLoadBalancingPolicy(serviceAndProto, loadBalancingPolicy);
    }

    public LoadBalancingPolicy getLoadBalancingPolicy(Pair<String, String> serviceAndProto) {
        return this.configuration.getLoadBalancingPolicy(serviceAndProto);
    }

    public void shutdownNow(ExecutorService executor) {
        List<Runnable> pendingTasks = executor.shutdownNow();
        // If some tasks were submitted to this executor but not yet commenced, make sure the corresponding futures complete
        for (Runnable pendingTask : pendingTasks) {
            if (pendingTask instanceof FutureTask<?>)
                ((FutureTask<?>) pendingTask).cancel(false);
        }
    }

    ListenableFuture<?> triggerOnDown(final Host host, boolean startReconnection) {
        return triggerOnDown(host, false, startReconnection);
    }

    public ListenableFuture<?> triggerOnDown(final Host host, final boolean isHostAddition, final boolean startReconnection) {
        return executor.submit(new ExceptionCatchingRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                onDown(host, isHostAddition, startReconnection);
            }
        });
    }

    public ListenableFuture<?> triggerOnAdd(final Host host) {
        return executor.submit(new ExceptionCatchingRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                onAdd(host);
            }
        });
    }

    public ListenableFuture<?> triggerOnUp(final Host host) {
        return executor.submit(new ExceptionCatchingRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                onUp(host);
            }
        });
    }

    protected void onAdd(final Host host) throws InterruptedException, ExecutionException {
        if (isClosed())
            return;

        logger.info("New host {} added", host);

        boolean locked = host.notificationsLock.tryLock(60, TimeUnit.SECONDS);
        if (!locked) {
            logger.warn("Could not acquire notifications lock within {} seconds, ignoring ADD notification for {}", 60, host);
            return;
        }

        try {
            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>();
            futures.add(maybeAddPool(host, blockingExecutor));

            // Only mark the node up once all session have added their pool (if the load-balancing
            // policy says it should), so that Host.isUp() don't return true before we're reconnected
            // to the node.
            ListenableFuture<List<Boolean>> f = Futures.allAsList(futures);
            List<Boolean> poolCreationResults = f.get();

            // If any of the creation failed, they will have signaled a connection failure
            // which will trigger a reconnection to the node. So don't bother marking UP.
            if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                logger.debug("Connection pool cannot be created, not marking {} UP", host);
                return;
            }

            host.setUp();

            for (Pair<String, String> serviceAndProto : host.getServices()) {
                getLoadBalancingPolicy(serviceAndProto).onAdd(host);
            }
        } finally {
            host.notificationsLock.unlock();
        }
    }

    protected void onUp(final Host host) throws InterruptedException, ExecutionException {
        // Note that in generalize we can parallelize the pool creation on
        // each session, but we shouldn't use executor since we're already
        // running on it most probably (and so we could deadlock). Use the
        // blockingExecutor instead, that's why it's for.
        onUp(host, blockingExecutor);
    }

    protected void onUp(final Host host, ListeningExecutorService poolCreatingExecutor) throws InterruptedException, ExecutionException {
        logger.debug("Host {} is UP", host);

        if (isClosed())
            return;

        boolean locked = host.notificationsLock.tryLock(60, TimeUnit.SECONDS);
        if (!locked) {
            logger.warn("Could not acquire notifications lock within {} seconds, ignoring UP notification for {}", 60, host);
            return;
        }
        try {
            if (host.state == Host.State.UP) {
                /**
                 * New services maybe added in the host, so we should on Up the new service
                 * loadbalancing policy. It doesn't matter we on up all the services
                 * because host is locked.
                 */
                for (Pair<String, String> serviceAndProto : host.getServices()) {
                    getLoadBalancingPolicy(serviceAndProto).onUp(host);
                }
                return;
            }

            // If there is a reconnection attempt scheduled for that node, cancel it
            Future<?> scheduleAttempt = host.reconnectionAttempt.getAndSet(null);
            if (scheduleAttempt != null) {
                logger.debug("Cancelling reconnection attempt since node is UP");
                scheduleAttempt.cancel(false);
            }

            removePool(host);
            for (Pair<String, String> serviceAndProto : host.getServices()) {
                getLoadBalancingPolicy(serviceAndProto).onUp(host);
            }

            logger.debug("Adding/renewing host pools for newly UP host {}", host);

            List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>();
            futures.add(forceRenewPool(host, poolCreatingExecutor));

            // Only mark the node up once all session have re-added their pool (if the load-balancing
            // policy says it should), so that Host.isUp() don't return true before we're reconnected
            // to the node.
            ListenableFuture<List<Boolean>> f = Futures.allAsList(futures);
            Futures.addCallback(f, new FutureCallback<List<Boolean>>() {
                @Override
                public void onSuccess(List<Boolean> poolCreationResults) {
                    // If any of the creation failed, they will have signaled a connection failure
                    // which will trigger a reconnection to the node. So don't bother marking UP.
                    if (Iterables.any(poolCreationResults, Predicates.equalTo(false))) {
                        logger.debug("Connection pool cannot be created, not marking {} UP", host);
                        return;
                    }

                    host.setUp();
                }

                @Override
                public void onFailure(Throwable t) {
                    // That future is not really supposed to throw unexpected exceptions
                    if (!(t instanceof InterruptedException))
                        logger.error("Unexpected error while marking node UP: while this shouldn't happen, this shouldn't be critical", t);
                }
            });

            f.get();
        } finally {
            host.notificationsLock.unlock();
        }
    }

    public void onDown(final Host host, final boolean isHostAddition, boolean startReconnection) throws InterruptedException, ExecutionException {
        logger.debug("Host {} is DOWN", host);

        if (isClosed())
            return;

        boolean locked = host.notificationsLock.tryLock(60, TimeUnit.SECONDS);
        if (!locked) {
            logger.warn("Could not acquire notifications lock within {} seconds, ignoring DOWN notification for {}", 60, host);
            return;
        }

        try {
            // Note: we don't want to skip that method if !host.isUp() because we set isUp
            // late in onUp, and so we can rely on isUp if there is an error during onUp.
            // But if there is a reconnection attempt in progress already, then we know
            // we've already gone through that method since the last successful onUp(), so
            // we're good skipping it.
            if (host.reconnectionAttempt.get() != null) {
                logger.debug("Aborting onDown because a reconnection is running on DOWN host {}", host);
                return;
            }

            host.setDown();

            for (Pair<String, String> serviceAndProto : host.getServices()) {
                getLoadBalancingPolicy(serviceAndProto).onDown(host);
            }
            removePool(host);

            logger.debug("{} is down, scheduling connection retries", host);
            startPeriodicReconnectionAttempt(host, isHostAddition);
        } finally {
            host.notificationsLock.unlock();
        }
    }

    public ListenableFuture<?> triggerOnServiceRemoval(final Host host, Pair<String, String> serviceAndProto) {
        return executor.submit(new ExceptionCatchingRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                removeServiceFromLoadBalancing(host, serviceAndProto);
            }
        });
    }

    public void removeServiceFromLoadBalancing(final Host host, Pair<String, String> serviceAndProto) throws InterruptedException {
        logger.debug("remove {} from host {}", serviceAndProto, host);

        if (isClosed())
            return;

        boolean locked = host.notificationsLock.tryLock(60, TimeUnit.SECONDS);
        if (!locked) {
            logger.warn("Could not acquire notifications lock within {} seconds, ignoring service removal notification for {}", 60, host);
            return;
        }

        host.removeService(serviceAndProto);
        getLoadBalancingPolicy(serviceAndProto).onDown(host);

        host.notificationsLock.unlock();
    }

    public abstract void startPeriodicReconnectionAttempt(final Host host, final boolean isHostAddition);

    public abstract String getPrefix();

    public abstract CloseFuture close();

    public abstract boolean isClosed();

    public abstract ListenableFuture<Boolean> forceRenewPool(final Host host, ListeningExecutorService executor);

    public abstract CloseFuture removePool(Host host);

    public abstract ListenableFuture<Boolean> maybeAddPool(final Host host, ListeningExecutorService executor);
}

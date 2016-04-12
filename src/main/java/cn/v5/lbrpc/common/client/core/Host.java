package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yangwei on 15-5-3.
 */

public class Host {
    public final ReentrantLock notificationsLock = new ReentrantLock(true);
    public final AtomicReference<ListenableFuture<?>> reconnectionAttempt = new AtomicReference<ListenableFuture<?>>();
    final InetSocketAddress address;
    // each host supports only one proto at one time, but there maybe many services on it
    final Set<Pair<String, String>> services = new CopyOnWriteArraySet<Pair<String, String>>();
    protected volatile State state;

    //private Manager manager;

    public Host(Pair<String, String> serviceAndProto, InetSocketAddress address) {
        services.add(serviceAndProto);
        this.address = address;
        //this.manager = manager;
        this.state = State.ADDED;
    }

    public void addService(Pair<String, String> serviceAndProto) {
        services.add(serviceAndProto);
    }

    public Collection<Pair<String, String>> getServices() {
        return services;
    }

    public void removeService(Pair<String, String> serviceAndProto) {
        services.remove(serviceAndProto);
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public boolean isUp() {
        return state == State.UP;
    }

    public boolean isDown() {
        return state == State.DOWN;
    }

    public boolean wasJustAdded() {
        return state == State.ADDED;
    }

    public void setUp() {
        this.state = State.UP;
    }

    public void setDown() {
        this.state = State.DOWN;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Host) {
            Host that = (Host) other;
            return this.address.equals(that.address);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public String toString() {
        return address.toString();
    }

    public enum State {ADDED, DOWN, UP}

    public interface StateListener {
        public void onUp(Host host);

        public void onDown(Host host);

        public void onAdd(Host host);
    }
}
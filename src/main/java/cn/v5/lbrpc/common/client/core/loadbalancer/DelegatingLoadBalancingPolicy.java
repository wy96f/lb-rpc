package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.client.core.Host;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by yangwei on 21/8/15.
 */
public abstract class DelegatingLoadBalancingPolicy implements LoadBalancingPolicy {
    protected final LoadBalancingPolicy delegate;

    public DelegatingLoadBalancingPolicy(LoadBalancingPolicy delegate) {
        this.delegate = delegate;
    }

    public void init(Collection<Host> hosts) {
        delegate.init(hosts);
    }

    public Iterator<Host> queryPlan() {
        return delegate.queryPlan();
    }

    public void onUp(Host host) {
        delegate.onUp(host);
    }

    public void onDown(Host host) {
        delegate.onDown(host);
    }

    public void onAdd(Host host) {
        delegate.onAdd(host);
    }

    public LoadBalancingPolicy getChildPolicy() {
        return delegate;
    }
}
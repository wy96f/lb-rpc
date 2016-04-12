package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.client.core.Host;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by yangwei on 15-5-3.
 */
public interface LoadBalancingPolicy extends Host.StateListener {
    public Iterator<Host> queryPlan();

    public void init(Collection<Host> hosts);
}

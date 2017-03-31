package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.data.IRequest;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangwei on 15-5-5.
 */
public class RoundRobinPolicy implements LoadBalancingPolicy {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinPolicy.class);
    private final AtomicInteger index = new AtomicInteger();

    private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();

    @Override
    public void init(Collection<Host> hosts) {
        // we use addAllAbsent since there maybe multiple same service proxys which init loadbalancing policy
        this.liveHosts.addAllAbsent(hosts);
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
    }

    @Override
    public Iterator<Host> queryPlan(IRequest request) {
        final List<Host> hosts = (List<Host>) liveHosts.clone();
        final int startIdx = index.getAndIncrement();
        if (startIdx > Integer.MAX_VALUE - 10000) {
            index.set(0);
        }

        return new AbstractIterator<Host>() {
            private int idx = startIdx;
            private int remaining = hosts.size();

            @Override
            protected Host computeNext() {
                if (remaining <= 0) {
                    return endOfData();
                }

                remaining--;
                int c = idx++ % hosts.size();
                if (c < 0) {
                    c += hosts.size();
                }
                return hosts.get(c);
            }
        };
    }

    @Override
    public void onUp(Host host) {
        liveHosts.addIfAbsent(host);
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onDown(Host host) {
        liveHosts.remove(host);
    }

    @Override
    public void onRemoval(Host host) {
        liveHosts.remove(host);
    }
}
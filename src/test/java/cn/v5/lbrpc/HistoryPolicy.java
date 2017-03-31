package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.client.core.loadbalancer.DelegatingLoadBalancingPolicy;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by yangwei on 21/8/15.
 */
public class HistoryPolicy extends DelegatingLoadBalancingPolicy {
    private static final Logger logger = LoggerFactory.getLogger(HistoryPolicy.class);

    public enum Action {INIT, UP, DOWN, ADD, REMOVE}

    List<Pair<Action, Host>> history = Lists.newArrayList();

    public HistoryPolicy(LoadBalancingPolicy delegate) {
        super(delegate);
    }

    @Override
    public void init(Collection<Host> hosts) {
        super.init(hosts);
        for (Host host : hosts) {
            logger.info("init host {}", host);
            history.add(Pair.create(Action.INIT, host));
        }
    }

    @Override
    public void onUp(Host host) {
        super.onUp(host);
        logger.info("up host {}", host);
        history.add(Pair.create(Action.UP, host));
    }

    @Override
    public void onDown(Host host) {
        super.onDown(host);
        logger.info("down host {}", host);
        history.add(Pair.create(Action.DOWN, host));
    }

    @Override
    public void onAdd(Host host) {
        super.onAdd(host);
        logger.info("add host {}", host);
        history.add(Pair.create(Action.ADD, host));
    }

    @Override
    public void onRemoval(Host host) {
        super.onRemoval(host);
        logger.info("remove host {}", host);
        history.add(Pair.create(Action.REMOVE, host));
    }
}

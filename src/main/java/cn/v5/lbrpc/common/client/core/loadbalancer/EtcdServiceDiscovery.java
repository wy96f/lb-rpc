package cn.v5.lbrpc.common.client.core.loadbalancer;

import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.InetSocketAddressUtil;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.justinsb.etcd.EtcdClient;
import com.justinsb.etcd.EtcdClientException;
import com.justinsb.etcd.EtcdNode;
import com.justinsb.etcd.EtcdResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

/**
 * Created by yangwei on 15-6-4.
 */
public class EtcdServiceDiscovery implements ServiceDiscovery {
    static final Gson gson = new GsonBuilder().create();
    private static final Logger logger = LoggerFactory.getLogger(EtcdServiceDiscovery.class);
    private static final String SERVICE_PREFIX = CBUtil.SERVICE_PREFIX;
    public final EtcdClient client;

    //protected ScheduledExecutorService scheduleExecutor = new ScheduledThreadPoolExecutor(1);

    //private final Map<String, InetSocketAddress> services = Maps.newHashMap();

    public EtcdServiceDiscovery(String url) {
        this.client = new EtcdClient(URI.create(url));
    }

    @Override
    public List<InetSocketAddress> getServerList(String service, String proto) throws IOException {
        EtcdResult result = client.listChildren("/" + proto + SERVICE_PREFIX + service);
        if (result.isError()) {
            throw new EtcdClientException(result.message, result.errorCode);
        }
        logger.debug("get service {} result {}", service, result);
        List<InetSocketAddress> addresses = Lists.newArrayList();
        if (result.node.nodes == null) {
            return Lists.newArrayList();
        }
        for (EtcdNode node : result.node.nodes) {
            int lastIdx = node.key.lastIndexOf("/");
            addresses.add(InetSocketAddressUtil.deserializeAddress(node.key.substring(lastIdx + 1)));
        }
        return addresses;
    }
}
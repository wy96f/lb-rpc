package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.utils.CBUtil;
import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 21/8/15.
 */
public class TestUtils {
    static Host findHost(String proto, InetSocketAddress address) {
        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(proto);
        Preconditions.checkNotNull(nodeClient, "Node client of " + proto + " is null");
        Host host = nodeClient.getManager().getHost(address);
        Preconditions.checkNotNull(host, "Not found host " + address);
        return host;
    }
}

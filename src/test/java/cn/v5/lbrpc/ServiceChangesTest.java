package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.loadbalancer.RoundRobinPolicy;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import cn.v5.lbrpc.thrift.Echo;
import cn.v5.lbrpc.thrift.EchoServiceImpl;
import cn.v5.lbrpc.thrift.NonExistentServiceImpl;
import cn.v5.lbrpc.thrift.ThriftNonExistentService;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by yangwei on 27/8/15.
 */
public class ServiceChangesTest {
    InMemoryServiceDiscoveryAndRegistration sdr;

    public CompositeServer server;
    public Echo.Iface echoService;

    HistoryPolicy historyPolicy;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr);
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC);
        this.historyPolicy = new HistoryPolicy(new RoundRobinPolicy());
        this.echoService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr)
                .withLoadBalancingPolicy(historyPolicy)
                .create(Echo.Iface.class, CBUtil.THRIFT_PROTO);
    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    @Test
    public void testServiceAddedWhenHostUpAfterDown() throws Exception {
        server.unregister(AbstractServerFactory.THRIFT_RPC);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }

        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC);
        server.register(new NonExistentServiceImpl(), AbstractServerFactory.THRIFT_RPC);

        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(CBUtil.THRIFT_PROTO);
        /**
         * Here we refresh EchoService first so we can up the EchoService first bcs host is not up.
         * If we refresh nonExistentService first, we can only rely on the reconnection to re add the EchoService.
         */

        //nodeClient.forceRefresh();
        // wait for the manager's onUp
        Thread.sleep(1000);
        HistoryPolicy nonExistentPolicy = new HistoryPolicy(new RoundRobinPolicy());
        ThriftNonExistentService.Iface nonExistentService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr)
                .withLoadBalancingPolicy(nonExistentPolicy)
                .create(ThriftNonExistentService.Iface.class, CBUtil.THRIFT_PROTO);

        echoService.echoReturnString("yw", "bll");
        nonExistentService.requestNonExistentService("yw");

        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())).isUp(), equalTo(true));
        Pair<HistoryPolicy.Action, Host> h1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h2 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h3 = Pair.create(HistoryPolicy.Action.UP,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        assertThat(historyPolicy.history, Matchers.contains(h1, h2, h3));

        Pair<HistoryPolicy.Action, Host> n1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        //Pair<HistoryPolicy.Action, Host> n2 = Pair.create(HistoryPolicy.Action.UP,
        //        new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        assertThat(nonExistentPolicy.history, Matchers.contains(n1));
    }

    @Test
    public void testServiceRemovedWhenHostUpAfterDown() throws Exception {
        int port = 60501;
        server.register(new NonExistentServiceImpl(), AbstractServerFactory.THRIFT_RPC);
        server.register(new NonExistentServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);
        HistoryPolicy nonExistentPolicy = new HistoryPolicy(new RoundRobinPolicy());
        ThriftNonExistentService.Iface nonExistentService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr)
                .withLoadBalancingPolicy(nonExistentPolicy)
                .create(ThriftNonExistentService.Iface.class, CBUtil.THRIFT_PROTO);

        server.unregister(AbstractServerFactory.THRIFT_RPC);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
            assertFalse(true);
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }

        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC);

        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(CBUtil.THRIFT_PROTO);
        /**
         * Here we refresh EchoService first so we can up the EchoService first bcs host is not up.
         * If we refresh nonExistentService first, we can only rely on the reconnection to re add the EchoService.
         */
        //nodeClient.forceRefresh();

        // wait for the manager's onUp
        Thread.sleep(1000);

        echoService.echoReturnString("yw", "bll");

        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())).isUp(), equalTo(true));
        Pair<HistoryPolicy.Action, Host> h1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h2 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h3 = Pair.create(HistoryPolicy.Action.UP,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        assertThat(historyPolicy.history, Matchers.contains(h1, h2, h3));

        Pair<HistoryPolicy.Action, Host> n1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> n2 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), port)));

        Pair<HistoryPolicy.Action, Host> n3 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        // There is up action bcs host is up
        Pair<HistoryPolicy.Action, Host> n4 = Pair.create(HistoryPolicy.Action.UP,
                new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> n5 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        nodeClient.forceRefresh();
        Thread.sleep(1000);
        assertThat(nonExistentPolicy.history, Matchers.contains(n1, n2, n3, n4, n5));
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())).getCurrentServices(), Matchers.contains(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO)));
    }
}

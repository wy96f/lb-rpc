package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.loadbalancer.RoundRobinPolicy;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import cn.v5.lbrpc.thrift.Echo;
import cn.v5.lbrpc.thrift.EchoServiceImpl;
import cn.v5.lbrpc.thrift.NonExistentServiceImpl;
import cn.v5.lbrpc.thrift.ThriftNonExistentService;
import cn.v5.lbrpc.thrift.server.ThriftRpcServerFactory;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by yangwei on 21/8/15.
 */
public class HostChangesTest {
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
    public void testNewHost() throws Exception {
        int newPort = 60051;
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, newPort);

        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(CBUtil.THRIFT_PROTO);
        nodeClient.forceRefresh();
        // wait for the manager's onAdd
        Thread.sleep(100);

        Pair<HistoryPolicy.Action, Host> h1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h2 = Pair.create(HistoryPolicy.Action.ADD, new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), newPort)));

        assertThat(historyPolicy.history, Matchers.contains(h1, h2));
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), newPort)).isUp(), equalTo(true));
    }

    @Test
    public void testHostDown() throws Exception {
        server.unregister(AbstractServerFactory.THRIFT_RPC);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }

        Pair<HistoryPolicy.Action, Host> h1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h2 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        assertThat(historyPolicy.history, Matchers.contains(h1, h2));

        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())).isDown(), equalTo(true));
    }

    @Test
    public void testHostUpAfterDown() throws Exception {
        server.unregister(AbstractServerFactory.THRIFT_RPC);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }

        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC);
        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(CBUtil.THRIFT_PROTO);
        nodeClient.forceRefresh();

        // wait for the manager's onUp
        Thread.sleep(100);

        echoService.echoReturnString("yw", "bll");

        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())).isUp(), equalTo(true));
        Pair<HistoryPolicy.Action, Host> h1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h2 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h3 = Pair.create(HistoryPolicy.Action.UP,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        assertThat(historyPolicy.history, Matchers.contains(h1, h2, h3));
    }

    @Test
    public void testHostUpWithOldConAfterDown() throws Exception {
        thrown.expect(NoHostAvailableException.class);

        server.unregister(AbstractServerFactory.THRIFT_RPC);
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC);

        // wait until onDown is triggered
        Thread.sleep(200);
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())).isUp(), equalTo(false));
        Pair<HistoryPolicy.Action, Host> h1 = Pair.create(HistoryPolicy.Action.INIT,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        Pair<HistoryPolicy.Action, Host> h2 = Pair.create(HistoryPolicy.Action.DOWN,
                new Host(Pair.create(ThriftUtils.getServiceName(Echo.Iface.class), CBUtil.THRIFT_PROTO), new InetSocketAddress(InetAddress.getLocalHost(), AbstractServerFactory.newFactory(AbstractServerFactory.THRIFT_RPC).getDefaultPort())));
        assertThat(historyPolicy.history, Matchers.contains(h1, h2));

        echoService.echoReturnString("yw", "bll");
    }
}
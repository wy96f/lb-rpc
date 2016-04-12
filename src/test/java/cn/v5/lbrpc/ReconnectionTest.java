package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.loadbalancer.RoundRobinPolicy;
import cn.v5.lbrpc.common.client.core.policies.BoundedExponentialReconnectionPolicy;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.thrift.Echo;
import cn.v5.lbrpc.thrift.EchoServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by yangwei on 24/8/15.
 */
public class ReconnectionTest {
    InMemoryServiceDiscoveryAndRegistration sdr;

    public CompositeServer server;
    public Echo.Iface echoService;

    HistoryPolicy historyPolicy;

    int baseInterval = 10;
    int retry = 10;
    int port = 60051;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr);
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);
        this.historyPolicy = new HistoryPolicy(new RoundRobinPolicy());
        this.echoService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr)
                .withLoadBalancingPolicy(historyPolicy)
                .withReconnectionPolicy(new BoundedExponentialReconnectionPolicy(baseInterval, baseInterval, retry))
                .create(Echo.Iface.class, CBUtil.THRIFT_PROTO);
    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    @Test
    public void testReconnection() throws Exception {
        server.unregister(AbstractServerFactory.THRIFT_RPC, port);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isDown(), equalTo(true));
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);

        // wait for the reconnection completed
        Thread.sleep(baseInterval * 4);
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isUp(), equalTo(true));
        // must not have exception thrown
        echoService.echoReturnString("yw", "bll");
    }

    @Test
    public void testEndedReconnection() throws Exception {
        server.unregister(AbstractServerFactory.THRIFT_RPC, port);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isDown(), equalTo(true));

        // sleep until reconnection ended
        Thread.sleep(baseInterval * (retry * 2));
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);

        // wait for the reconnection if that still works
        Thread.sleep(baseInterval * 4);
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isDown(), equalTo(true));
    }

    @Test
    public void testForceRefreshAfterEndedReconnection() throws Exception {
        server.unregister(AbstractServerFactory.THRIFT_RPC, port);

        try {
            // trigger manager's onDown
            echoService.echoReturnString("yw", "bll");
        } catch (Exception e) {
            assertTrue(e instanceof NoHostAvailableException);
        }
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isDown(), equalTo(true));

        // sleep until reconnection ended
        Thread.sleep(baseInterval * (retry * 2));
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);

        // wait for the reconnection if that still works
        Thread.sleep(baseInterval * 4);
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isDown(), equalTo(true));

        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(CBUtil.THRIFT_PROTO);
        nodeClient.forceRefresh();
        // wait for the manager's onUp
        Thread.sleep(100);
        assertThat(TestUtils.findHost(CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port)).isUp(), equalTo(true));
    }
}

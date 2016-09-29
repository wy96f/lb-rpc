package cn.v5.lbrpc.thrift;

import cn.v5.lbrpc.InMemoryServiceDiscoveryAndRegistration;
import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by yangwei on 21/7/15.
 */
public class ThriftShortHandTest {
    public CompositeServer server;
    public Echo.Iface echoService;
    public InMemoryServiceDiscoveryAndRegistration sdr;
    int port = 60080;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr);
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);
        this.echoService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr).create(Echo.Iface.class, CBUtil.THRIFT_PROTO);
    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    @Test
    public void testEchoReturnString() throws TException {
        String res = echoService.echoReturnString("echo", "string");
        assertThat(res, equalTo("echo" + "string"));
    }

    @Test
    public void testEchoReturnVoid() throws TException {
        echoService.echoReturnVoid(1.0, 2);
    }

    @Test
    public void testEchoWithoutParams() throws TException {
        assertThat(echoService.echoWithoutParams(), equalTo(ImmutableList.of(EchoServiceImpl.echoResponse)));
    }

    @Test
    public void testEchoSerializedException() throws TException {
        thrown.expect(RpcException.class);
        thrown.expectMessage("SerializedException");
        echoService.echoSerializedException(ByteBuffer.wrap("serial".getBytes()));
    }

    @Test
    public void testEchoRuntimeException() throws TException {
        thrown.expect(RpcException.class);
        thrown.expectMessage("Unexpected error occurred server side");
        echoService.echoRuntimeException(Sets.newHashSet());
    }

    @Test
    public void testEchoTimeout() throws TException {
        thrown.expect(RpcException.class);
        thrown.expectMessage("All host(s) tried for query failed");
        echoService.echoTimeout(Maps.newHashMap());
    }

    // null return is not supported by thrift(see <a href="http://wiki.apache.org/thrift/ThriftFeatures"></a>)
    @Test
    public void testEchoReturnNull() throws TException {
        thrown.expect(RpcInternalError.class);
        thrown.expectMessage("Unexpected error while processing response");
        echoService.echoReturnNull();
    }

    @Test
    public void testEchoWithNullParameter() throws TException {
        assertThat(echoService.echoWithNullParameter(null), equalTo("null"));
    }

    @Test
    public void testNonExistentService() throws Exception {
        thrown.expect(RpcException.class);
        thrown.expectMessage("Unexpected error occurred server side");

        sdr.registerServer(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port));
        ThriftNonExistentService.Iface nonExistentService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr).create(ThriftNonExistentService.Iface.class, CBUtil.THRIFT_PROTO);
        nonExistentService.requestNonExistentService("yw");
    }
}
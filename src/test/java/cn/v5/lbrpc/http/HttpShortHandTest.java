package cn.v5.lbrpc.http;

import cn.v5.lbrpc.InMemoryServiceDiscoveryAndRegistration;
import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.http.utils.HttpUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by yangwei on 21/7/15.
 */
public class HttpShortHandTest {
    public CompositeServer server;
    public RestEchoService restEchoService;
    InMemoryServiceDiscoveryAndRegistration sdr;
    int port = 60061;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr);
        server.register(new RestEchoServiceImpl(), AbstractServerFactory.TOMCAT_CONTAINER, port);
        this.restEchoService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr).create(RestEchoService.class, CBUtil.HTTP_PROTO);
    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    @Test
    public void testEchoPost() {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        RestEchoInfo res = restEchoService.echoPost(echoInfo);
        assertThat(res.getMessage(), equalTo(RestEchoServiceImpl.hello + "ep"));
        assertThat(res.getX(), is(RestEchoServiceImpl.num + 1));
    }

    @Test
    public void testEchoPut() {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        RestEchoInfo res = restEchoService.echoPut(echoInfo);
        assertThat(res.getMessage(), equalTo(RestEchoServiceImpl.hello + "ep"));
        assertThat(res.getX(), is(RestEchoServiceImpl.num + 1));
    }

    @Test
    public void testEchoGet() {
        String res = restEchoService.echoGet("eg");
        assertThat(res, equalTo(RestEchoServiceImpl.hello + "eg"));
    }

    @Test
    public void testEchoPostIOException() throws IOException {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        thrown.expect(RpcException.class);
        thrown.expectMessage("HTTP 500 Internal Server Error");
        restEchoService.echoPostIOException(echoInfo);
    }

    @Test
    public void testEchoGetRuntimeException() {
        thrown.expect(RpcException.class);

        thrown.expectMessage("HTTP 500 Internal Server Error");
        restEchoService.echoGetRuntimeException("eg");
    }

    @Test
    public void testEchoPutTimeout() {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        thrown.expect(NoHostAvailableException.class);
        thrown.expectMessage("Read timed out");
        restEchoService.echoPutTimeout(echoInfo);
    }

    @Test
    public void testEchoReturnNull() {
        restEchoService.echoReturnNull();
    }

    @Test
    public void testEchoReturnVoid() {
        restEchoService.echoReturnVoid();
    }

    // null parameter is not supported by rest easy
    @Test
    public void testEchoWithNullParameter() {
        thrown.expect(RpcException.class);
        thrown.expectMessage("java.lang.IllegalArgumentException: Arguments must not be null");
        restEchoService.echoWithNullParameter(null);
    }

    @Test
    public void testNonExistentService() throws Exception {
        thrown.expect(NoHostAvailableException.class);
        thrown.expectMessage("HTTP 404 Not Found");

        sdr.registerServer(HttpUtils.getServiceName(HttpNonExistentService.class), CBUtil.HTTP_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port));
        HttpNonExistentService nonExistentService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr).create(HttpNonExistentService.class, CBUtil.HTTP_PROTO);
        nonExistentService.requestNonExistentService();
    }
}

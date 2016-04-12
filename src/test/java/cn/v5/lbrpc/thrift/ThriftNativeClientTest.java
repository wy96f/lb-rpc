package cn.v5.lbrpc.thrift;

import cn.v5.lbrpc.InMemoryServiceDiscoveryAndRegistration;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Created by yangwei on 27/7/15.
 */
public class ThriftNativeClientTest {
    public CompositeServer server;
    public Echo.Client echoService;
    public TTransport transport;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        InMemoryServiceDiscoveryAndRegistration sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr);
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC);

        InetSocketAddress address = sdr.getServerList("Echo", AbstractServerFactory.THRIFT_RPC).get(0);
        this.transport = new TFramedTransport(new TSocket(address.getHostString(), address.getPort()));
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol multiplexedProtocol = new TMultiplexedProtocol(binaryProtocol, "Echo");
        transport.open();
        this.echoService = new Echo.Client(multiplexedProtocol, multiplexedProtocol);
    }

    @After
    public void tearDown() {
        transport.close();
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
        echoService.echoWithoutParams();
    }

    @Test
    public void testEchoSerializedException() throws TException {
        thrown.expect(SerializedException.class);
        echoService.echoSerializedException(ByteBuffer.wrap("serial".getBytes()));
    }

    @Test
    public void testEchoRuntimeException() throws TException {
        thrown.expect(TApplicationException.class);
        thrown.expectMessage("echoResponse");
        echoService.echoRuntimeException(Sets.newHashSet());
    }

    @Test
    public void testEchoTimeout() throws TException {
        echoService.echoTimeout(Maps.newHashMap());
    }

    // null return is not supported by thrift(see <a href="http://wiki.apache.org/thrift/ThriftFeatures"></a>)
    @Test
    public void testEchoReturnNull() throws TException {
        thrown.expect(TApplicationException.class);
        thrown.expectMessage("echoReturnNull failed: unknown result");
        echoService.echoReturnNull();
    }

    @Test
    public void testEchoWithNullParameter() throws TException {
        Assert.assertThat(echoService.echoWithNullParameter(null), CoreMatchers.equalTo("null"));
    }
}

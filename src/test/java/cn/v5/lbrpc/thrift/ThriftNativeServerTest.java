package cn.v5.lbrpc.thrift;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.client.core.loadbalancer.FixedServiceDiscovery;
import cn.v5.lbrpc.common.utils.CBUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by yangwei on 27/7/15.
 */
public class ThriftNativeServerTest {
    public Thread serverThread;
    public TServer tServer;
    public Echo.Iface echoService;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.serverThread = new Thread() {
            @Override
            public void run() {
                try {
                    Echo.Iface impl = new EchoServiceImpl();

                    // for THsHaServer
                    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(9527);

                    TMultiplexedProcessor processor = new TMultiplexedProcessor();
                    processor.registerProcessor("Echo", new Echo.Processor(impl));

                    tServer = new THsHaServer(new THsHaServer.Args(serverTransport).transportFactory(new TFramedTransport.Factory()).processor(processor));

                    System.out.println("Start server on port 9527...");
                    tServer.serve();

                } catch (TTransportException e) {
                    e.printStackTrace();
                }
            }
        };

        serverThread.start();
        while (tServer == null || !tServer.isServing()) {
            Thread.sleep(500);
        }

        this.echoService = RpcProxyFactory.createBuilder().withServiceDiscovery(new FixedServiceDiscovery(new InetSocketAddress("localhost", 9527))).create(Echo.Iface.class, CBUtil.THRIFT_PROTO);
    }

    @After
    public void tearDown() throws Exception {
        RpcProxyFactory.close();
        tServer.stop();
        serverThread.join();
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
        thrown.expect(RpcException.class);
        thrown.expectMessage("SerializedException");
        echoService.echoSerializedException(ByteBuffer.wrap("serial".getBytes()));
    }

    // native thrift server will close client if RuntimeException is thrown
    @Test
    public void testEchoRuntimeException() throws TException {
        thrown.expect(RpcException.class);
        thrown.expectMessage("All host(s) tried for query failed");
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
}
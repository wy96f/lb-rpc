package cn.v5.lbrpc.thrift;

import cn.v5.lbrpc.InMemoryServiceDiscoveryAndRegistration;
import cn.v5.lbrpc.common.client.RpcContext;
import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by yangwei on 8/6/16.
 */
public class ThriftAsyncTest {
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
        this.echoService = RpcProxyFactory.createBuilder().withServiceDiscovery(sdr).createAsync(Echo.Iface.class, CBUtil.THRIFT_PROTO);
    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    private void waitUntilFutureFinished(ListenableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            System.out.println(e);
            assertTrue(false);
        } catch (ExecutionException e) {
            System.out.println(e);
            assertTrue(false);
        }
    }

    @Test
    public void testEchoReturnString() throws TException, ExecutionException, InterruptedException {
        echoService.echoReturnString("echo", "string");
        ListenableFuture<String> future = RpcContext.getContext().<String>getResult();
        assertEquals("echo" + "string", future.get());
    }

    @Test
    public void testEchoReturnVoid() throws TException {
        echoService.echoReturnVoid(1.0, 2);
        ListenableFuture<Void> future = RpcContext.getContext().<Void>getResult();
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                assertTrue(true);
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println("testEchoReturnVoid onFailure: " + t);
                assertTrue(false);
            }
        });
        waitUntilFutureFinished(future);
    }

    @Test
    public void testEchoSerializedException() throws TException {
//        thrown.expect(ExecutionException.class);
//        thrown.expectMessage("SerializedException");
        echoService.echoTimeout(new HashMap<>());
        echoService.echoReturnNull();
        try {
            echoService.echoRuntimeException(new HashSet<>());
            echoService.echoSerializedException(ByteBuffer.wrap("serial".getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
        }
//        Thread.sleep(1000);
        //ListenableFuture<Boolean> future = RpcContext.getContext().<Boolean>getResult();
        //future.get();
    }
}

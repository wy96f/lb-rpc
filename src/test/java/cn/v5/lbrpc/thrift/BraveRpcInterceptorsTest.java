package cn.v5.lbrpc.thrift;

import cn.v5.lbrpc.InMemoryServiceDiscoveryAndRegistration;
import cn.v5.lbrpc.SpanCollectorForTesting;
import cn.v5.lbrpc.TestUtils;
import cn.v5.lbrpc.common.client.BraveRpcClientInterceptor;
import cn.v5.lbrpc.common.client.RpcContext;
import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.BraveRpcServerInterceptor;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.LocalTracer;
import com.github.kristofa.brave.SpanId;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.twitter.zipkin.gen.Span;
import org.apache.thrift.TException;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Created by yangwei on 27/9/16.
 */
public class BraveRpcInterceptorsTest {
    public CompositeServer server;
    public Echo.Iface echoService;
    public Echo.Iface echoServiceAsync;

    public InMemoryServiceDiscoveryAndRegistration sdr;
    int port = 60080;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Brave brave;

    TestUtils.ExplicitSampler sampler = new TestUtils.ExplicitSampler();

    @Before
    public void setUp() throws Exception {
        sampler.setEnableSampling(true);
        SpanCollectorForTesting.getInstance().clear();

        final Brave.Builder builder = new Brave.Builder("thrift");
        brave = builder.spanCollector(SpanCollectorForTesting.getInstance()).traceSampler(sampler).build();

        this.sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr, new BraveRpcServerInterceptor(brave));
        server.register(new EchoServiceImpl(), AbstractServerFactory.THRIFT_RPC, port);
        this.echoService = RpcProxyFactory.createBuilder().
                withServiceDiscovery(sdr).
                withInterceptors(new BraveRpcClientInterceptor(brave)).
                create(Echo.Iface.class, CBUtil.THRIFT_PROTO);
        this.echoServiceAsync = RpcProxyFactory.createBuilder()
                .withServiceDiscovery(sdr)
                .withInterceptors(new BraveRpcClientInterceptor(brave))
                .createAsync(Echo.Iface.class, CBUtil.THRIFT_PROTO);

    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    @Test
    public void testEchoReturnString() throws Exception {
        String res = echoService.echoReturnString("echo", "string");
        TestUtils.validateSpans("echo:echoreturnstring");
    }

    @Test
    public void testEchoReturnStringAsync() throws Exception {
        echoServiceAsync.echoReturnString("echo", "string");
        ListenableFuture<String> future = RpcContext.getContext().<String>getResult();
        assertEquals("echo" + "string", future.get());
        TestUtils.validateSpans("echo:echoreturnstring");

    }

    @Test
    public void testEchoSerializedException() throws TException, UnknownHostException {
        try {
            echoService.echoSerializedException(ByteBuffer.wrap("serial".getBytes()));
            assertTrue(false);
        } catch (RpcException e) {
            assertThat(e.getClass().equals(RpcException.class));
            assertThat(e.getMessage().startsWith("SerializedException"));
        }
        TestUtils.validateSpans("echo:echoserializedexception");
    }

    @Test
    public void testEchoRuntimeException() throws TException, UnknownHostException {
        try {
            echoService.echoRuntimeException(Sets.newHashSet());
            assertTrue(false);
        } catch (RpcException e) {
            assertThat(e.getClass().equals(RpcException.class));
            assertThat(e.getMessage().startsWith("Unexpected error occurred server side"));
        }
        TestUtils.validateSpans("echo:echoruntimeexception");
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Span serverSpan = spans.get(0);
        assertThat(serverSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals(BraveRpcServerInterceptor.RPC_EXCEPTION))
                .extracting(b -> new String(b.value))
                .has(new Condition<>(b -> b.matches("^org.apache.thrift.TApplicationException: java.lang.RuntimeException.*$"), "RuntimeException thrown"), atIndex(0));

    }

    @Test
    public void testEchoTimeout() throws TException, UnknownHostException {
        try {
            echoService.echoTimeout(Maps.newHashMap());
            assertTrue(false);
        } catch (RpcException e) {
            assertThat(e.getClass().equals(RpcException.class));
            assertThat(e.getMessage().startsWith("All host(s) tried for query failed"));
        }
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        assertEquals(spans.size(), 1);
        Span clientSpan = spans.get(0);
        TestUtils.validateSpan(clientSpan, Arrays.asList("cs", "cr"), "echo:echotimeout");
        assertThat(clientSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals(BraveRpcServerInterceptor.RPC_EXCEPTION))
                .extracting(b -> new String(b.value))
                .has(new Condition<>(b -> b.matches("^cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException:.*Timed out waiting for server respons.*$"), "NoHostAvailableException thrown"), atIndex(0));
    }

    // null return is not supported by thrift(see <a href="http://wiki.apache.org/thrift/ThriftFeatures"></a>)
    @Test
    public void testEchoReturnNull() throws TException, UnknownHostException {
        try {
            echoService.echoReturnNull();
            assertTrue(false);
        } catch (RpcInternalError e) {
            assertThat(e.getClass().equals(RpcInternalError.class));
            assertThat(e.getMessage().startsWith("Unexpected error while processing response"));
        }
        TestUtils.validateSpans("echo:echoreturnnull");
    }

    @Test
    public void testNonExistentService() throws Exception {
        sdr.registerServer(ThriftUtils.getServiceName(ThriftNonExistentService.Iface.class), CBUtil.THRIFT_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port));
        ThriftNonExistentService.Iface nonExistentService = RpcProxyFactory.createBuilder()
                .withServiceDiscovery(sdr)
                .withInterceptors(new BraveRpcClientInterceptor(brave))
                .create(ThriftNonExistentService.Iface.class, CBUtil.THRIFT_PROTO);
        try {
            nonExistentService.requestNonExistentService("yw");
            assertTrue(false);
        } catch (RpcException e) {
            assertThat(e.getClass().equals(RpcException.class));
            assertThat(e.getMessage().startsWith("Unexpected error occurred server side"));
        }
        TestUtils.validateSpans("thriftnonexistentservice:requestnonexistentservice");
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Span serverSpan = spans.get(0);
        assertThat(serverSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals(BraveRpcServerInterceptor.RPC_EXCEPTION))
                .extracting(b -> new String(b.value))
                .has(new Condition<>(b -> b.matches("^org.apache.thrift.TApplicationException: Service name not found: ThriftNonExistentService.*$"), "RuntimeException thrown"), atIndex(0));

    }

    @Test
    public void testExistingTraceId() throws TException, UnknownHostException {
        LocalTracer localTracer = brave.localTracer();
        SpanId spanId = localTracer.startNewSpan("localSpan", "myop");
        String res = echoService.echoReturnString("echo", "string");
        assertThat(res.equals("echo" + "string"));
        TestUtils.validateSpans("echo:echoreturnstring");
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Optional<Span> maybeSpan = spans.stream()
                .filter(s -> s.getAnnotations().stream().anyMatch(a -> "ss".equals(a.value)))
                .findFirst();
        assertTrue("Could not find expected server span", maybeSpan.isPresent());
        Span span = maybeSpan.get();
        //Verify that the localTracer's trace id and span id were propagated to the server
        assertThat(span.getTrace_id()).isEqualTo(spanId.traceId);
        assertThat(span.getParent_id()).isEqualTo(spanId.spanId);
    }

    @Test
    public void testSamplingDisabled() throws TException {
        sampler.setEnableSampling(false);
        String res = echoService.echoReturnString("echo", "string");
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        assertThat(spans.size()).isEqualTo(0);
        assertThat(res.equals("echo" + "string"));
    }
}

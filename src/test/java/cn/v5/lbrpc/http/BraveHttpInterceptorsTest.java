package cn.v5.lbrpc.http;

import cn.v5.lbrpc.InMemoryServiceDiscoveryAndRegistration;
import cn.v5.lbrpc.SpanCollectorForTesting;
import cn.v5.lbrpc.TestUtils;
import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.http.utils.HttpUtils;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.http.HttpRequest;
import com.github.kristofa.brave.http.SpanNameProvider;
import com.github.kristofa.brave.jaxrs2.BraveClientRequestFilter;
import com.github.kristofa.brave.jaxrs2.BraveClientResponseFilter;
import com.github.kristofa.brave.jaxrs2.BraveContainerRequestFilter;
import com.github.kristofa.brave.jaxrs2.BraveContainerResponseFilter;
import com.twitter.zipkin.gen.Span;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by yangwei on 29/9/16.
 */
public class BraveHttpInterceptorsTest {
    public CompositeServer server;
    public RestEchoService restEchoService;
    InMemoryServiceDiscoveryAndRegistration sdr;
    int port = 60061;

    @Rule
    public TestName name = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Brave brave;

    TestUtils.ExplicitSampler sampler = new TestUtils.ExplicitSampler();

    SpanNameProvider spanNameProvider = new SpanNameProvider() {
        @Override
        public String spanName(HttpRequest httpRequest) {
            return httpRequest.getHttpMethod() + ":" + httpRequest.getUri().getPath();
        }
    };

    @Before
    public void setUp() throws Exception {
        sampler.setEnableSampling(true);
        SpanCollectorForTesting.getInstance().clear();

        final Brave.Builder builder = new Brave.Builder();
        brave = builder.spanCollector(SpanCollectorForTesting.getInstance()).traceSampler(sampler).build();

        this.sdr = new InMemoryServiceDiscoveryAndRegistration();
        this.server = new CompositeServer(sdr,
                new BraveContainerRequestFilter(brave.serverRequestInterceptor(), spanNameProvider),
                new BraveContainerResponseFilter(brave.serverResponseInterceptor()));
        server.register(new RestEchoServiceImpl(), AbstractServerFactory.TOMCAT_CONTAINER, port);
        this.restEchoService = RpcProxyFactory.createBuilder()
                .withServiceDiscovery(sdr)
                .withInterceptors(new BraveClientRequestFilter(spanNameProvider, brave.clientRequestInterceptor()),
                                new BraveClientResponseFilter(brave.clientResponseInterceptor()))
                .create(RestEchoService.class, CBUtil.HTTP_PROTO);
    }

    @After
    public void tearDown() {
        RpcProxyFactory.close();
        server.close();
    }

    @Test
    public void testEchoPost() throws UnknownHostException {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        RestEchoInfo res = restEchoService.echoPost(echoInfo);
        assertThat(res.getMessage(), equalTo(RestEchoServiceImpl.hello + "ep"));
        assertThat(res.getX(), is(RestEchoServiceImpl.num + 1));
        TestUtils.validateSpans("post:/echoservice/echo_post", true);

        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Span serverSpan = spans.get(0);
        Assertions.assertThat(serverSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals("http.status_code"))
                .extracting(b -> new String(b.value))
                .containsExactly("200");
    }


    @Test
    public void testEchoPostIOException() throws IOException {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        try {
            restEchoService.echoPostIOException(echoInfo);
            assertTrue(false);
        } catch (RpcException e) {
            Assertions.assertThat(e.getClass().equals(RpcException.class));
            Assertions.assertThat(e.getMessage().startsWith("HTTP 500 Internal Server Error"));
        }

        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Assertions.assertThat(spans.size()).isEqualTo(1);
        Span clientSpan = spans.get(0);
        TestUtils.validateSpan(clientSpan, Arrays.asList("cs", "cr"), "post:/echoservice/echo_post_io_exception");
        Assertions.assertThat(clientSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals("http.status_code"))
                .extracting(b -> new String(b.value))
                .containsExactly("500");
    }

    @Test
    public void testEchoGetRuntimeException() {
        try {
            restEchoService.echoGetRuntimeException("eg");
            assertTrue(false);
        } catch (RpcException e) {
            Assertions.assertThat(e.getClass().equals(RpcException.class));
            Assertions.assertThat(e.getMessage().startsWith("HTTP 500 Internal Server Error"));
        }

        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Assertions.assertThat(spans.size()).isEqualTo(1);
        Span clientSpan = spans.get(0);
        TestUtils.validateSpan(clientSpan, Arrays.asList("cs", "cr"), "get:/echoservice/echo_get_runtime_exception");
        Assertions.assertThat(clientSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals("http.status_code"))
                .extracting(b -> new String(b.value))
                .containsExactly("500");
    }

    @Test
    public void testEchoPutTimeout() {
        RestEchoInfo echoInfo = new RestEchoInfo();
        echoInfo.setMessage("ep");
        echoInfo.setX(1);
        try {
            restEchoService.echoPutTimeout(echoInfo);
            assertTrue(false);
        } catch (NoHostAvailableException e) {
            Assertions.assertThat(e.getClass().equals(NoHostAvailableException.class));
            Assertions.assertThat(e.getMessage().startsWith("Read timed out"));
        }
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Assertions.assertThat(spans.size()).isEqualTo(0);
    }

    // null parameter is not supported by rest easy
    @Test
    public void testEchoWithNullParameter() {
        try {
            restEchoService.echoWithNullParameter(null);
            assertTrue(false);
        } catch (RpcException e) {
            Assertions.assertThat(e.getClass().equals(RpcException.class));
            Assertions.assertThat(e.getMessage().startsWith("java.lang.IllegalArgumentException: Arguments must not be null"));
        }
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Assertions.assertThat(spans.size()).isEqualTo(0);
    }

    @Test
    public void testNonExistentService() throws Exception {
        sdr.registerServer(HttpUtils.getServiceName(HttpNonExistentService.class), CBUtil.HTTP_PROTO, new InetSocketAddress(InetAddress.getLocalHost(), port));
        HttpNonExistentService nonExistentService = RpcProxyFactory.createBuilder()
                .withServiceDiscovery(sdr)
                .withInterceptors(new BraveClientRequestFilter(spanNameProvider, brave.clientRequestInterceptor()),
                        new BraveClientResponseFilter(brave.clientResponseInterceptor()))
                .create(HttpNonExistentService.class, CBUtil.HTTP_PROTO);

        try {
            nonExistentService.requestNonExistentService();
            assertTrue(false);
        } catch (NoHostAvailableException e) {
            Assertions.assertThat(e.getClass().equals(NoHostAvailableException.class));
            Assertions.assertThat(e.getMessage().startsWith("HTTP 404 Not Found"));
        }

        TestUtils.validateSpans("get:/echoservice/request_non_existent_service", true);
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        Span serverSpan = spans.get(0);
        Assertions.assertThat(serverSpan.getBinary_annotations())
                .filteredOn(b -> b.key.equals("http.status_code"))
                .extracting(b -> new String(b.value))
                .containsExactly("404");
    }
}
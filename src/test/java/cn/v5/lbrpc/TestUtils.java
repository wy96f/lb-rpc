package cn.v5.lbrpc;

import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.server.BraveRpcServerInterceptor;
import com.github.kristofa.brave.Sampler;
import com.google.common.base.Preconditions;
import com.twitter.zipkin.gen.Span;
import org.assertj.core.api.Condition;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;


/**
 * Created by yangwei on 21/8/15.
 */
public class TestUtils {
    static Host findHost(String proto, InetSocketAddress address) {
        AbstractNodeClient nodeClient = RpcProxyFactory.nodeClients.get(proto);
        Preconditions.checkNotNull(nodeClient, "Node client of " + proto + " is null");
        Host host = nodeClient.getManager().getHost(address);
        Preconditions.checkNotNull(host, "Not found host " + address);
        return host;
    }

    public static class ExplicitSampler extends Sampler {
        boolean enableSampling;

        public void setEnableSampling(boolean enableSampling) {
            this.enableSampling = enableSampling;
        }

        @Override
        public boolean isSampled(long traceId) {
            return enableSampling;
        }
    }

    public static void validateSpans(String spanName) throws UnknownHostException {
        validateSpans(spanName, false);
    }

    public static void validateSpans(String spanName, boolean http) throws UnknownHostException {
        List<Span> spans = SpanCollectorForTesting.getInstance().getCollectedSpans();
        assertThat(spans.size()).isEqualTo(2);

        Span serverSpan = spans.get(0);
        assertThat(serverSpan.getTrace_id()).isEqualTo(spans.get(1).getTrace_id());
        validateSpan(serverSpan, Arrays.asList("sr", "ss"), spanName);

        InetAddress address = InetAddress.getLocalHost();

        if (!http) {
            assertThat(serverSpan.getBinary_annotations())
                    .filteredOn(b -> b.key.equals(BraveRpcServerInterceptor.RCP_REMOTE_ADDRESS))
                    .extracting(b -> new String(b.value))
                    .has(new Condition<>(b -> b.matches("^/" + address.getHostAddress() + ":[\\d]+$"), "a local ip address"), atIndex(0));
        } else {
            assertThat(serverSpan.getBinary_annotations())
                    .filteredOn(b -> b.key.equals("http.url"))
                    .extracting(b -> new String(b.value))
                    .has(new Condition<>(b -> b.matches("^http://" + address.getHostName() + ":[\\d]+.*"), "a local ip address"), atIndex(0));

        }

        validateSpan(spans.get(1), Arrays.asList("cs", "cr"), spanName);
    }

    public static void validateSpan(Span span, Iterable<String> expectedAnnotations, String spanName) {
        assertThat(span.getName()).isEqualTo(spanName);
        assertThat(span.getAnnotations()).extracting(a -> a.value).containsExactlyElementsOf(expectedAnnotations);
    }
}

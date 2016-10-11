package cn.v5.lbrpc.common.server;

import com.github.kristofa.brave.*;
import com.github.kristofa.brave.http.BraveHttpHeaders;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.github.kristofa.brave.IdConversion.convertToLong;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by yangwei on 26/9/16.
 */
public class BraveRpcServerInterceptor implements ServerInterceptor {
    public static final String RCP_REMOTE_ADDRESS = "rpc.remote_addr";
    public static final String RPC_EXCEPTION = "rpc.exception";

    private final ServerRequestInterceptor serverRequestInterceptor;
    private final ServerResponseInterceptor serverResponseInterceptor;

    public BraveRpcServerInterceptor(Brave brave) {
        this.serverRequestInterceptor = checkNotNull(brave.serverRequestInterceptor());
        this.serverResponseInterceptor = checkNotNull(brave.serverResponseInterceptor());
    }

    @Override
    public void preProcess(String fullMethod, SocketAddress address, Map<String, String> header) {
        serverRequestInterceptor.handle(new RpcServerRequestAdapter(fullMethod, address, header));
    }

    @Override
    public void postProcess(Exception e) {
        serverResponseInterceptor.handle(new RpcServerResponseAdapter(e));
    }

    private static class RpcServerRequestAdapter implements ServerRequestAdapter {
        private final String serviceAndMethod;
        private final SocketAddress address;
        private final Map<String, String> requestHeaders;

        public RpcServerRequestAdapter(String serviceAndMethod, SocketAddress address, Map<String, String> header) {
            this.serviceAndMethod = serviceAndMethod;
            this.address = address;
            this.requestHeaders = header;
        }

        @Override
        public TraceData getTraceData() {
            final String sampled = requestHeaders.get(BraveHttpHeaders.Sampled.getName());
            if (sampled != null) {
                if (sampled.equals("0") || sampled.toLowerCase().equals("false")) {
                    return TraceData.builder().sample(false).build();
                } else {
                    final String parentSpanId = requestHeaders.get(BraveHttpHeaders.ParentSpanId.getName());
                    final String traceId = requestHeaders.get(BraveHttpHeaders.TraceId.getName());
                    final String spanId = requestHeaders.get(BraveHttpHeaders.SpanId.getName());
                    if (traceId != null && spanId != null) {
                        SpanId span = getSpanId(traceId, spanId, parentSpanId);
                        return TraceData.builder().sample(true).spanId(span).build();
                    }
                }
            }
            return TraceData.builder().build();
        }

        @Override
        public String getSpanName() {
            return serviceAndMethod;
        }

        @Override
        public Collection<KeyValueAnnotation> requestAnnotations() {
            return address == null ? Collections.emptyList() :
                    Collections.singleton(KeyValueAnnotation.create(RCP_REMOTE_ADDRESS, address.toString()));
        }
    }

    private static class RpcServerResponseAdapter implements ServerResponseAdapter {
        private final Exception e;

        public RpcServerResponseAdapter(Exception e) {
            this.e = e;
        }

        @Override
        public Collection<KeyValueAnnotation> responseAnnotations() {
            return e == null ? Collections.emptyList() :
                    Collections.singletonList(KeyValueAnnotation.create(RPC_EXCEPTION, e.toString()));
        }
    }

    static SpanId getSpanId(String traceId, String spanId, String parentSpanId) {
        return SpanId.builder()
                .traceId(convertToLong(traceId))
                .spanId(convertToLong(spanId))
                .parentId(parentSpanId == null ? null : convertToLong(parentSpanId)).build();
    }
}

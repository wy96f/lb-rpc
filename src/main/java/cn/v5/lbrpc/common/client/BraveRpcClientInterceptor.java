package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.Connection.ResponseCallback;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.github.kristofa.brave.*;
import com.github.kristofa.brave.http.BraveHttpHeaders;
import com.google.gson.Gson;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;

import java.net.InetSocketAddress;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by yangwei on 21/9/16.
 */
public class BraveRpcClientInterceptor implements ClientInterceptor {
    public static final String RPC_ERROR = "rpc.error";
    public static final String RPC_RETRY_COUNT = "rpc.retry_count";
    public static final String RPC_EXCEPTION = "rpc.exception";


    private final ClientRequestInterceptor clientRequestInterceptor;
    private final ClientResponseInterceptor clientResponseInterceptor;
    private final ClientSpanThreadBinder clientSpanThreadBinder;

    public BraveRpcClientInterceptor(Brave brave) {
        this.clientRequestInterceptor = checkNotNull(brave.clientRequestInterceptor());
        this.clientResponseInterceptor = checkNotNull(brave.clientResponseInterceptor());
        this.clientSpanThreadBinder = checkNotNull(brave.clientSpanThreadBinder());
    }

    @Override
    public <T extends IRequest, V extends IResponse> ResponseCallback<T, V> intercept(ResponseCallback<T, V> request) {
        return new ForwardingClientCall<T, V>(request) {
            @Override
            public void sendRequest(RequestHandler.ResultSetCallback callback) {
                clientRequestInterceptor.handle(new RpcClientRequestAdapter<>(callback.request()));
                final Span currentClientSpan = clientSpanThreadBinder.getCurrentClientSpan();
                super.sendRequest(new ForwardingClientCallListener<T, V>(callback) {
                    @Override
                    public void onClose(Map<InetSocketAddress, Throwable> error, int retryCount, Exception e) {
                        try {
                            clientSpanThreadBinder.setCurrentSpan(currentClientSpan);
                            clientResponseInterceptor.handle(new RpcClientResponseAdapter(error, retryCount, e));
                            super.onClose(error, retryCount, e);
                        } finally {
                            clientSpanThreadBinder.setCurrentSpan(null);
                        }
                    }
                });
            }
        };
    }

    private static final class RpcClientRequestAdapter<T extends IRequest> implements ClientRequestAdapter {
        private final T req;

        public RpcClientRequestAdapter(T request) {
            this.req = request;
        }

        @Override
        public String getSpanName() {
            return req.getService() + ":" + req.getMethod();
        }

        @Override
        public void addSpanIdToRequest(SpanId spanId) {
            if (spanId == null) {
                req.setHeader(BraveHttpHeaders.Sampled.getName(), "0");
            } else {
                req.setHeader(BraveHttpHeaders.Sampled.getName(), "1");
                req.setHeader(BraveHttpHeaders.TraceId.getName(), IdConversion.convertToString(spanId.traceId));
                req.setHeader(BraveHttpHeaders.SpanId.getName(), IdConversion.convertToString(spanId.spanId));
                if (spanId.nullableParentId() != null) {
                    req.setHeader(BraveHttpHeaders.ParentSpanId.getName(), IdConversion.convertToString(spanId.parentId));
                }
            }
        }

        @Override
        public Collection<KeyValueAnnotation> requestAnnotations() {
            return Collections.emptyList();
        }

        @Override
        public Endpoint serverAddress() {
            return null;
        }
    }

    private static final class RpcClientResponseAdapter implements ClientResponseAdapter {
        private final Map<InetSocketAddress, Throwable> error;
        private final int retryCount;
        private final Exception e;

        public RpcClientResponseAdapter(Map<InetSocketAddress, Throwable> error, int retryCount, Exception e) {
            this.error = error;
            this.retryCount = retryCount;
            this.e = e;
        }

        @Override
        public Collection<KeyValueAnnotation> responseAnnotations() {
            List<KeyValueAnnotation> keyValueAnnotations = new ArrayList<>();
            if (error != null && !error.isEmpty()) {
                keyValueAnnotations.add(KeyValueAnnotation.create(RPC_ERROR, makeMessage(error)));
            }
            keyValueAnnotations.add(KeyValueAnnotation.create(RPC_RETRY_COUNT, Integer.toString(retryCount)));
            if (e != null) {
                keyValueAnnotations.add(KeyValueAnnotation.create(RPC_EXCEPTION, e.toString()));
            }
            return keyValueAnnotations;
        }

        private String makeMessage(Map<InetSocketAddress, Throwable> errors) {

            if (errors.size() <= 3) {
                StringBuilder sb = new StringBuilder();
                sb.append("Host(s) tried for query failed (tried: ");
                int n = 0;
                for (Map.Entry<InetSocketAddress, Throwable> entry : errors.entrySet()) {
                    if (n++ > 0) sb.append(", ");
                    sb.append(entry.getKey()).append(" (").append(entry.getValue()).append(')');
                }
                return sb.append(')').toString();
            }
            return String.format("Host(s) tried for query failed (tried: %s)", errors.keySet());
        }
    }

}
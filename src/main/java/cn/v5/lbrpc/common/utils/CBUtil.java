package cn.v5.lbrpc.common.utils;

import cn.v5.lbrpc.common.client.ClientInterceptor;
import cn.v5.lbrpc.common.server.BraveRpcServerInterceptor;
import com.twitter.zipkin.gen.Span;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yangwei on 15-5-4.
 */
public abstract class CBUtil {
    public static final ByteBufAllocator allocator = new PooledByteBufAllocator(true);

    public static final String HTTP_PROTO = "http";
    public static final String PROTOBUF_PROTO = "protobuf";
    public static final String THRIFT_PROTO = "thrift";

    public static final String SERVICE_PREFIX = "/services/";

    public static <T> List<T> convertToClientInterceptor(List<Object> interceptors) {
        List<T> res = new ArrayList<>();
        for (Object interceptor : interceptors) {
            res.add((T) interceptor);
        }
        return res;
    }
}

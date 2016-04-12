package cn.v5.lbrpc.common.utils;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Created by yangwei on 15-5-4.
 */
public abstract class CBUtil {
    public static final ByteBufAllocator allocator = new PooledByteBufAllocator(true);

    public static final String HTTP_PROTO = "http";
    public static final String PROTOBUF_PROTO = "protobuf";
    public static final String THRIFT_PROTO = "thrift";

    public static final String SERVICE_PREFIX = "/services/";

}

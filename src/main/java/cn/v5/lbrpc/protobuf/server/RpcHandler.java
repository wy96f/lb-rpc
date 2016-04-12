package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.protobuf.data.ProtobufMessage;

/**
 * Created by yangwei on 15-5-8.
 */
public interface RpcHandler {
    ProtobufMessage execute(ProtobufMessage request) throws Exception;
}

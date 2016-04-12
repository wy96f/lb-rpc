package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.protobuf.client.ProtobufRpcMethodInfo;
import cn.v5.lbrpc.protobuf.data.ProtobufMessage;

/**
 * Created by yangwei on 15-5-8.
 */
public class RpcMethodHandler implements RpcHandler {
    private final Object target;
    private final ProtobufRpcMethodInfo methodInfo;

    public RpcMethodHandler(Object target, ProtobufRpcMethodInfo methodInfo) {
        this.target = target;
        this.methodInfo = methodInfo;
    }

    @Override
    public ProtobufMessage execute(ProtobufMessage request) throws Exception {
        Object[] param;
        if (request.getData() != null) {
            param = new Object[]{methodInfo.outputDecode(request.getData())};
        } else {
            param = new Object[]{null};
        }

        ProtobufMessage protobufMessage = new ProtobufMessage();
        Object ret = methodInfo.getMethod().invoke(target, param);
        if (ret == null) {
            return protobufMessage;
        }

        protobufMessage.setData(methodInfo.inputEncode(ret));

        return protobufMessage;
    }
}

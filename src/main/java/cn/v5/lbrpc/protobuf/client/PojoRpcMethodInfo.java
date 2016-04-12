package cn.v5.lbrpc.protobuf.client;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.jprotobuf.pbrpc.ProtobufRPC;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-5-4.
 */
public class PojoRpcMethodInfo extends ProtobufRpcMethodInfo {
    private Codec inputCodec;
    private Codec outputCodec;

    public PojoRpcMethodInfo(Method method, ProtobufRPC protobufRPC) {
        super(method);

        Class<?> inputClass = getInputClasses()[0];
        if (inputClass != null) {
            inputCodec = ProtobufProxy.create(inputClass);
        }
        Class<?> outputClass = getOutputClass();
        if (outputClass != null) {
            outputCodec = ProtobufProxy.create(outputClass);
        }
    }

    @Override
    public byte[] inputEncode(Object input) throws IOException {
        if (inputCodec != null) {
            return inputCodec.encode(input);
        }
        return null;
    }

    @Override
    public Object outputDecode(byte[] output) throws IOException {
        if (outputCodec != null && output != null) {
            return outputCodec.decode(output);
        }
        return null;
    }
}

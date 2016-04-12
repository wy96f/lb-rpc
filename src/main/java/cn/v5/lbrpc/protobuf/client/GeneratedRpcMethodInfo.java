package cn.v5.lbrpc.protobuf.client;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.google.protobuf.GeneratedMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-5-4.
 */
public class GeneratedRpcMethodInfo extends ProtobufRpcMethodInfo {
    private static final String PROTOBUF_PARSE_METHOD = "parseFrom";

    private Method parseFromMethod;

    public GeneratedRpcMethodInfo(Method method, ProtobufRPC protobufRPC) {
        super(method);

        Class<?> outputClass = getOutputClass();
        if (outputClass != null) {
            if (GeneratedMessage.class.isAssignableFrom(outputClass)) {
                try {
                    parseFromMethod = outputClass.getMethod(PROTOBUF_PARSE_METHOD, InputStream.class);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public byte[] inputEncode(Object input) throws IOException {
        if (input instanceof GeneratedMessage) {
            return ((GeneratedMessage) input).toByteArray();
        }
        return null;
    }

    @Override
    public Object outputDecode(byte[] output) throws IOException {
        Class<?> outputClass = getOutputClass();
        if (parseFromMethod != null && output != null) {
            try {
                return parseFromMethod.invoke(outputClass, new ByteArrayInputStream(output));
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }
        return null;
    }
}

package cn.v5.lbrpc.protobuf.client;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import com.google.protobuf.GeneratedMessage;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-6-24.
 */
public abstract class ProtobufRpcMethodInfo extends AbstractRpcMethodInfo {

    protected ProtobufRpcMethodInfo(Method method) {
        super(method);
        if (getInputClasses().length > 1) {
            throw new IllegalArgumentException("RPC method can not has more than one parameter. illegal method:"
                    + method.getName());
        }
    }

    public abstract byte[] inputEncode(Object input) throws IOException;

    public static boolean isMessageType(Method method) {

        boolean paramMessagetType = false;

        Class<?>[] types = method.getParameterTypes();
        if (types.length == 1) {
            if (GeneratedMessage.class.isAssignableFrom(types[0])) {
                paramMessagetType = true;
            }
        }

        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && returnType != Void.class) {
            if (GeneratedMessage.class.isAssignableFrom(returnType)) {
                if (paramMessagetType) {
                    return true;
                } else {
                    throw new IllegalArgumentException("Invalid RPC method. parameter type and return "
                            + "type should define in same way.");
                }
            }
        }

        return false;
    }
}

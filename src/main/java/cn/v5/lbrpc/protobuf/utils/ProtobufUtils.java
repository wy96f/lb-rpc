package cn.v5.lbrpc.protobuf.utils;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;

import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-6-29.
 */
public class ProtobufUtils {
    public static String getServiceName(Class<?> interfaceClass) {
        String serviceName = null;
        for (Method method : interfaceClass.getMethods()) {
            ProtobufRPC protobufRPC = method.getAnnotation(ProtobufRPC.class);
            if (protobufRPC != null) {
                serviceName = protobufRPC.serviceName();
                break;
            }
        }
        return serviceName;
    }
}

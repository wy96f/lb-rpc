package cn.v5.lbrpc.protobuf.server;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.protobuf.client.GeneratedRpcMethodInfo;
import cn.v5.lbrpc.protobuf.client.PojoRpcMethodInfo;
import cn.v5.lbrpc.protobuf.client.ProtobufRpcMethodInfo;
import com.baidu.bjf.remoting.protobuf.utils.StringUtils;
import com.baidu.jprotobuf.pbrpc.ProtobufRPCService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-5-8.
 */
public class RpcServiceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(RpcServiceRegistry.class);
    private final ProtobufRpcServer server;
    private Map<String, RpcHandler> serviceMap = Maps.newConcurrentMap();

    public RpcServiceRegistry(ProtobufRpcServer server) {
        this.server = server;
    }

    public RpcHandler lookupService(String serviceName, String method) {
        return serviceMap.get(serviceName + "!" + method);
    }

    public void registerService(final Object target) {
        Preconditions.checkNotNull(target, "register null service");

        Class<?> targetClass = target.getClass();
        Method[] methods = targetClass.getMethods();
        String serviceName = null;
        for (Method method : methods) {
            ProtobufRPCService protobufPRCService = method.getAnnotation(ProtobufRPCService.class);
            if (protobufPRCService != null) {
                serviceName = protobufPRCService.serviceName();
                doRegisterMethod(method, target, protobufPRCService);
            }
        }
        server.registration.registerServer(serviceName, CBUtil.PROTOBUF_PROTO, server.socket);
    }

    public void unregisterServices() {
        List<ListenableFuture<?>> futureList = Lists.newArrayList();
        for (String service : serviceMap.keySet()) {
            ListenableFuture<?> unregisterFuture = server.registration.unregisterServer(service, CBUtil.PROTOBUF_PROTO, server.socket);
            if (unregisterFuture != null) {
                futureList.add(unregisterFuture);
            }
        }
        try {
            Futures.successfulAsList(futureList).get(4000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while unregistering thrift services", e.getCause());
        } catch (TimeoutException e) {
            logger.warn("Time out while unregistering thrift services", e);
        }
    }

    private void doRegisterMethod(Method method, Object service, ProtobufRPCService protobufRPCService) {
        ProtobufRpcMethodInfo methodInfo;

        String serviceName = protobufRPCService.serviceName();
        Preconditions.checkNotNull(serviceName, "register service name null");

        String methodName = protobufRPCService.methodName();
        if (StringUtils.isEmpty(methodName)) {
            methodName = method.getName();
        }

        if (!ProtobufRpcMethodInfo.isMessageType(method)) {
            // using POJO
            methodInfo = new PojoRpcMethodInfo(method, null);

        } else {
            // support google protobuf GeneratedMessage
            methodInfo = new GeneratedRpcMethodInfo(method, null);
        }

        methodInfo.setServiceName(serviceName);
        methodInfo.setMethodName(methodName);

        serviceMap.put(serviceName + "!" + methodName, new RpcMethodHandler(service, methodInfo));
    }
}

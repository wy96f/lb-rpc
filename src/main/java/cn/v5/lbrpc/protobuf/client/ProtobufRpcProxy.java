package cn.v5.lbrpc.protobuf.client;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.client.AbstractRpcProxy;
import cn.v5.lbrpc.common.client.IProxy;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import cn.v5.lbrpc.protobuf.data.ProtobufMessage;
import cn.v5.lbrpc.protobuf.data.ProtobufRequestMeta;
import cn.v5.lbrpc.protobuf.utils.ProtobufUtils;
import com.baidu.bjf.remoting.protobuf.utils.StringUtils;
import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangwei on 15-5-3.
 */
public class ProtobufRpcProxy<T> implements IProxy<T, ProtobufMessage> {
    /**
     * target interface class
     */
    public final Class<T> interfaceClass;
    public final AbstractNodeClient protobufNodeClient;
    /**
     * target interface class
     */
    public final String serviceName;
    public final LoadBalancingPolicy loadBalancingPolicy;
    public Map<String, AbstractRpcMethodInfo> cachedRpcMethods = new HashMap<String, AbstractRpcMethodInfo>();
    public AbstractRpcProxy invocationProxy;

    public ProtobufRpcProxy(AbstractNodeClient protobufNodeClient, Class<T> interfaceClass, LoadBalancingPolicy loadBalancingPolicy) {
        this.interfaceClass = interfaceClass;
        this.protobufNodeClient = protobufNodeClient;
        this.serviceName = ProtobufUtils.getServiceName(interfaceClass);
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    @Override
    public void setInvocationProxy(AbstractRpcProxy invocationProxy) {
        this.invocationProxy = invocationProxy;
    }

    @Override
    public Pair<String, String> getServiceAndProto() {
        return Pair.create(serviceName, CBUtil.PROTOBUF_PROTO);
    }

    @Override
    public T proxy() throws Exception {
        {
            Method[] methods = interfaceClass.getMethods();
            String lastServiceName = null;
            for (Method method : methods) {
                ProtobufRPC protobufRPC = method.getAnnotation(ProtobufRPC.class);
                if (protobufRPC != null) {
                    String serviceName = protobufRPC.serviceName();
                    if (lastServiceName != null) {
                        Preconditions.checkArgument(serviceName.compareTo(lastServiceName) == 0, "service name in one class should be same");
                    }
                    lastServiceName = serviceName;
                    String methodName = protobufRPC.methodName();
                    if (StringUtils.isEmpty(methodName)) {
                        methodName = method.getName();
                    }

                    String methodSig = serviceName + "!" + methodName;
                    if (cachedRpcMethods.containsKey(methodSig)) {
                        throw new IllegalArgumentException(
                                "Method already defined service name [" + serviceName
                                        + "] method name [" + methodName + "]");
                    }

                    AbstractRpcMethodInfo methodInfo;
                    if (!ProtobufRpcMethodInfo.isMessageType(method)) {
                        // using POJO
                        methodInfo = new PojoRpcMethodInfo(method, protobufRPC);

                    } else {
                        // support google protobuf GeneratedMessage
                        methodInfo = new GeneratedRpcMethodInfo(method, protobufRPC);
                    }

                    methodInfo.setServiceName(serviceName);
                    methodInfo.setMethodName(methodName);
                    cachedRpcMethods.put(methodSig, methodInfo);
                }
            }

            // if not protobufRpc method defined throw exception
            if (cachedRpcMethods.isEmpty()) {
                throw new IllegalArgumentException("There is no protobufRpc method in interface class:"
                        + interfaceClass.getName());
            }

            List<InetSocketAddress> initContactPoints = protobufNodeClient.addService(lastServiceName);
            // init service releated
            new RequestHandler<ProtobufMessage, ProtobufMessage>(protobufNodeClient).init(Pair.create(serviceName, CBUtil.PROTOBUF_PROTO), initContactPoints, loadBalancingPolicy);

            return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class[]{interfaceClass}, invocationProxy);
        }
    }

    @Override
    public AbstractRpcMethodInfo getMethodInfo(Method method) {
        String methodName = method.getName();
        String methodSignature = serviceName + '!' + methodName;
        AbstractRpcMethodInfo abstractRpcMethodInfo = cachedRpcMethods.get(methodSignature);
        return abstractRpcMethodInfo;
    }

    @Override
    public ProtobufMessage makeRequestMessage(AbstractRpcMethodInfo methodInfo, Object[] args) throws IOException {
        ProtobufMessage message = new ProtobufMessage();
        ProtobufRequestMeta requestMeta = new ProtobufRequestMeta();
        requestMeta.setMethodName(methodInfo.getMethodName());
        requestMeta.setServiceName(methodInfo.getServiceName());
        message.setRequest(requestMeta);
        if (args[0] != null) {
            message.setData(((ProtobufRpcMethodInfo) methodInfo).inputEncode(args[0]));
        }
        return message;
    }
}

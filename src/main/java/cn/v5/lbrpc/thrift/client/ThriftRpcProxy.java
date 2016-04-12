package cn.v5.lbrpc.thrift.client;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.client.AbstractRpcProxy;
import cn.v5.lbrpc.common.client.IProxy;
import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import cn.v5.lbrpc.thrift.data.ThriftMessage;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Primitives;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangwei on 15-6-19.
 */
public class ThriftRpcProxy<T> implements IProxy<T, ThriftMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ThriftRpcProxy.class);

    /**
     * target interface class
     */
    public final Class<T> interfaceClass;
    public final AbstractNodeClient abstractNodeClient;
    /**
     * target interface class
     */
    public final String serviceName;
    public final LoadBalancingPolicy loadBalancingPolicy;
    public Map<String, AbstractRpcMethodInfo> cachedRpcMethods = new HashMap<String, AbstractRpcMethodInfo>();
    public AbstractRpcProxy invocationProxy;

    public ThriftRpcProxy(AbstractNodeClient abstractNodeClient, Class<T> interfaceClass, LoadBalancingPolicy loadBalancingPolicy) {
        this.interfaceClass = interfaceClass;
        this.abstractNodeClient = abstractNodeClient;
        this.serviceName = ThriftUtils.getServiceName(interfaceClass);
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    @Override
    public void setInvocationProxy(AbstractRpcProxy invocationProxy) {
        this.invocationProxy = invocationProxy;
    }

    @Override
    public Pair<String, String> getServiceAndProto() {
        return Pair.create(serviceName, CBUtil.THRIFT_PROTO);
    }

    public T proxy() throws IOException {
        Method[] methods = interfaceClass.getMethods();
        for (Method method : methods) {
            String methodName = method.getName();

            String methodSig = serviceName + "!" + methodName;
            if (cachedRpcMethods.containsKey(methodSig)) {
                throw new IllegalArgumentException(
                        "Method already defined service name [" + serviceName
                                + "] method name [" + methodName + "]");
            }

            AbstractRpcMethodInfo methodInfo = new ThriftRpcMethodInfo(method);

            methodInfo.setServiceName(serviceName);
            methodInfo.setMethodName(methodName);
            cachedRpcMethods.put(methodSig, methodInfo);
        }

        if (cachedRpcMethods.isEmpty()) {
            throw new IllegalArgumentException("This no thrift method in interface class:"
                    + interfaceClass.getName());
        }

        List<InetSocketAddress> initContactPoints = abstractNodeClient.addService(serviceName);
        // init service releated
        new RequestHandler<ThriftMessage, ThriftMessage>(abstractNodeClient).init(Pair.create(serviceName, CBUtil.THRIFT_PROTO), initContactPoints, loadBalancingPolicy);

        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class[]{interfaceClass}, invocationProxy);
    }

    @Override
    public AbstractRpcMethodInfo getMethodInfo(Method method) {
        String methodName = method.getName();
        String methodSignature = serviceName + '!' + methodName;
        AbstractRpcMethodInfo abstractRpcMethodInfo = cachedRpcMethods.get(methodSignature);
        return abstractRpcMethodInfo;
    }

    public ThriftMessage makeRequestMessage(AbstractRpcMethodInfo methodInfo, Object[] args) throws IOException {
        ThriftMessage message = new ThriftMessage(methodInfo.getServiceName(), methodInfo.getMethodName(), TMessageType.CALL);

        Class<?> argsClass = ThriftUtils.getArgsClass(methodInfo.getMethod());

        Object thriftArgs = null;
        try {
            thriftArgs = argsClass.getConstructor(methodInfo.getInputClasses()).newInstance(args);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(String.format("init service %s method %s args failed",
                    methodInfo.getServiceName(), methodInfo.getMethodName()), e);
        }
        message.setArgs((TBase) thriftArgs);

        return message;
    }
}

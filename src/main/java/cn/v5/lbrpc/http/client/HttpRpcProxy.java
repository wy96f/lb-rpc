package cn.v5.lbrpc.http.client;

import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import cn.v5.lbrpc.http.client.core.HttpRequestHandler;
import cn.v5.lbrpc.http.client.core.RequestContext;

import javax.ws.rs.Path;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangwei on 15-6-8.
 */
public class HttpRpcProxy<T> implements InvocationHandler {
    private final AbstractNodeClient httpNodeClient;

    private final Class<T> interfaceClass;
    private final LoadBalancingPolicy loadBalancingPolicy;
    private String serviceName;
    private final List<Object> interceptors = new ArrayList<>();

    public HttpRpcProxy(AbstractNodeClient httpNodeClient, List<Object> interceptors, Class<T> interfaceClass, LoadBalancingPolicy loadBalancingPolicy) {
        this.httpNodeClient = httpNodeClient;
        this.interceptors.addAll(interceptors);
        this.interfaceClass = interfaceClass;
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    public T proxy() throws Exception {
        // validate http interface
        Path path = interfaceClass.getAnnotation(Path.class);
        if (path == null) {
            throw new IllegalArgumentException("This is not httpRpc proxy class:"
                    + interfaceClass.getName());
        }
        this.serviceName = interfaceClass.getSimpleName();

        List<InetSocketAddress> initContactPoints = httpNodeClient.addService(serviceName);
        new HttpRequestHandler(httpNodeClient).init(Pair.create(serviceName, CBUtil.HTTP_PROTO), initContactPoints, loadBalancingPolicy);

        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class[]{interfaceClass}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        RequestContext context = new RequestContext(serviceName, interfaceClass, method, args);
        return new HttpRequestHandler(httpNodeClient, interceptors, context).sendRequest();
    }
}

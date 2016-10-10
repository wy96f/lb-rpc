package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.*;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by yangwei on 15-5-3.
 */
public abstract class AbstractRpcProxy<T, V extends IRequest, S extends IResponse> implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRpcProxy.class);

    private final IProxy<T, V> proxyImpl;

    private final AbstractNodeClient<V, S> nodeClient;

    private final List<ClientInterceptor> interceptors = new ArrayList<>();

    public AbstractRpcProxy(AbstractNodeClient<V, S> nodeClient, List<ClientInterceptor> interceptors, IProxy<T, V> proxyImpl) {
        this.proxyImpl = proxyImpl;
        this.nodeClient = nodeClient;
        this.interceptors.addAll(interceptors);
        Collections.reverse(this.interceptors);
    }

    public abstract Object invoke(Object proxy, Method method, Object[] args) throws Throwable;

    public T proxy() throws Exception {
        return proxyImpl.proxy();
    }

    public Object invokeSpecificMethods(Object proxy, Method method, Object[] args) throws IOException {
        if (method.getName().equals("equals")) {
            return proxy == args[0];
        } else if (method.getName().equals("hashCode")) {
            return System.identityHashCode(proxy);
        } else if (method.getName().equals("toString") && (args == null || args.length == 0)) {
            return proxy.getClass().getName() + "@" + proxy.hashCode() +
                    ", with InvocationHandler " + this;
        }
        return null;
    }

    public ResultFuture<T> executeAsync(Object proxy, Method method, Object[] args) throws IOException {
        AbstractRpcMethodInfo abstractRpcMethodInfo = proxyImpl.getMethodInfo(method);
        if (abstractRpcMethodInfo == null) {
            throw new IllegalAccessError("Can not invoke method '" + method.getName()
                    + "' due to not a protbufRpc method.");
        }

        V message = proxyImpl.makeRequestMessage(abstractRpcMethodInfo, args);

        DefaultResultFuture<T, V, S> future = new DefaultResultFuture<>(message, abstractRpcMethodInfo);
        execute(proxyImpl.getServiceAndProto(), future);
        return future;
    }

    private Connection.ResponseCallback<V, S> getRequestHandler(Pair<String, String> serviceAndProto) {
        Connection.ResponseCallback res = new RequestHandler<V, S>(this.nodeClient, serviceAndProto);
        for (int i = 0; i < interceptors.size(); i++) {
            res = interceptors.get(i).intercept(res);
        }
        return res;
    }

    // TODO use delegate?
    private void execute(Pair<String, String> serviceAndProto, RequestHandler.ResultSetCallback callback) {
        Connection.ResponseCallback<V, S> requestHandler = getRequestHandler(serviceAndProto);
        requestHandler.sendRequest(callback);
    }
}

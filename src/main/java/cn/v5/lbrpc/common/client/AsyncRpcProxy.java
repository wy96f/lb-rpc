package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.google.common.base.Defaults;
import com.google.common.util.concurrent.Futures;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Created by yangwei on 15-6-29.
 */
public class AsyncRpcProxy<T, V extends IRequest, S extends IResponse> extends AbstractRpcProxy<T, V, S> {
    public AsyncRpcProxy(AbstractNodeClient<V, S> abstractNodeClient, List<ClientInterceptor> interceptors, IProxy<T, V> proxyImpl) {
        super(abstractNodeClient, interceptors, proxyImpl);
        proxyImpl.setInvocationProxy(this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object res = invokeSpecificMethods(proxy, method, args);
        if (res == null) {
            RpcContext.getContext().setResult(executeAsync(proxy, method, args));
        } else {
            RpcContext.getContext().setResult(Futures.immediateFuture(res));
        }
        if (method.getGenericReturnType() instanceof Class) {
            return Defaults.defaultValue(method.getReturnType());
        } else {
            return null;
        }
    }
}

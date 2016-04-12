package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.utils.Pair;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-6-29.
 */
public interface IProxy<T, V extends IRequest> {
    public T proxy() throws Exception;

    public V makeRequestMessage(AbstractRpcMethodInfo methodInfo, Object[] args) throws IOException;

    public AbstractRpcMethodInfo getMethodInfo(Method method);

    public Pair<String, String> getServiceAndProto();

    public void setInvocationProxy(AbstractRpcProxy invocationProxy);
}
package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;

/**
 * Created by yangwei on 21/9/16.
 */
public interface ClientInterceptor {
    public <T extends IRequest, V extends IResponse> Connection.ResponseCallback<T, V> intercept(Connection.ResponseCallback<T, V> request);
}

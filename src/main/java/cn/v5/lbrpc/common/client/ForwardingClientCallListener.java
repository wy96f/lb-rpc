package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by yangwei on 21/9/16.
 */
public abstract class ForwardingClientCallListener<Request extends IRequest, Response extends IResponse> implements RequestHandler.ResultSetCallback<Request, Response> {
    private final RequestHandler.ResultSetCallback<Request, Response> listener;

    public ForwardingClientCallListener(RequestHandler.ResultSetCallback<Request, Response> listener) {
        this.listener = listener;
    }


    @Override
    public Request request() {
        return listener.request();
    }

    @Override
    public void register(RequestHandler handler) {
        listener.register(handler);
    }

    @Override
    public boolean onSet(Connection connection, Response response) {
        return listener.onSet(connection, response);
    }

    @Override
    public void onException(Connection connection, Exception exception, int retryCount) {
        listener.onException(connection, exception, retryCount);
    }

    @Override
    public void onClose(Map<InetSocketAddress, Throwable> error, int retryCount, Exception e) {
        listener.onClose(error, retryCount, e);
    }
}
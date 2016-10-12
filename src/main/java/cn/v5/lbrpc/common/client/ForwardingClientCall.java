package cn.v5.lbrpc.common.client;

import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;

/**
 * Created by yangwei on 21/9/16.
 */
public abstract class ForwardingClientCall<Request extends IRequest, Response extends IResponse> implements Connection.ResponseCallback<Request, Response> {
    private final Connection.ResponseCallback<Request, Response> delegate;

    public ForwardingClientCall(Connection.ResponseCallback<Request, Response> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int retryCount() {
        return delegate.retryCount();
    }

    @Override
    public Request request() {
        return delegate.request();
    }

    @Override
    public void onSet(Connection connection, Response response, int retryCount) {
        delegate.onSet(connection, response, retryCount);
    }

    @Override
    public void onException(Connection connection, Exception exception, int retryCount) {
        delegate.onException(connection, exception, retryCount);
    }

    @Override
    public boolean onTimeout(Connection connection, int streamId, int retryCount) {
        return delegate.onTimeout(connection, streamId, retryCount);
    }

    @Override
    public void sendRequest(RequestHandler.ResultSetCallback callback) {
        delegate.sendRequest(callback);
    }
}

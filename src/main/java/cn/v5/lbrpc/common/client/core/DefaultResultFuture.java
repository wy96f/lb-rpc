package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yangwei on 15-5-4.
 */
public class DefaultResultFuture<T, Request extends IRequest, Response extends IResponse> extends AbstractFuture<T> implements ResultFuture<T>, RequestHandler.ResultSetCallback<Request, Response> {
    private final Request message;
    private final AbstractRpcMethodInfo abstractRpcMethodInfo;
    private volatile RequestHandler handler;

    public DefaultResultFuture(Request message, AbstractRpcMethodInfo abstractRpcMethodInfo) {
        this.message = message;
        this.abstractRpcMethodInfo = abstractRpcMethodInfo;
    }

    static RuntimeException extractCauseFromExecutionException(ExecutionException e) {
        if (e.getCause() instanceof RpcException)
            return ((RpcException) e.getCause());
        else
            return new RpcInternalError("Unexpected exception thrown", e.getCause());
    }

    /*@Override
    public int retryCount() {
        // does not matter
        return 0;
    }*/

    public RequestHandler getHandler() {
        return handler;
    }

    @Override
    public Request request() {
        return message;
    }

    @Override
    public T getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    @Override
    public T getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    public AbstractRpcMethodInfo getAbstractRpcMethodInfo() {
        return abstractRpcMethodInfo;
    }

    @Override
    public void register(RequestHandler handler) {
        this.handler = handler;
    }

    /*@Override
    public void onSet(Connection connection, Response response, int retryCount) {
        onSet(connection, response);
    }
*/
    @Override
    public boolean set(T value) {
        return super.set(value);
    }

    // make setException public
    @Override
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }

    @Override
    public boolean onSet(Connection connection, Response response) {
        try {
            return response.onResponse(connection, this);
/*            if (ErrCodes.isSuccess(response.getResponse().getErrorCode())) {
                set((T) rpcMethodInfo.outputDecode(response.getData()));
            } else {
                setException(new RpcInternalError(String.format("Unexpected error occurred server side on %s: %d %s",
                        connection.address, response.getResponse().getErrorCode(), response.getResponse().getErrorText())));
            }*/
        } catch (IOException e) {
            setException(new RpcInternalError("Unexpected error while processing response from " + connection.address, e));
            return false;
        }
    }

    @Override
    public void onException(Connection connection, Exception exception, int retryCount) {
        setException(exception);
    }

    @Override
    public void onClose(Map<InetSocketAddress, Throwable> error, int retryCount, Exception e) {

    }

    /*@Override
    public boolean onTimeout(Connection connection, int retryCount) {
        setException(new OperationTimedOutException(connection.address));
        return true;
    }*/
}

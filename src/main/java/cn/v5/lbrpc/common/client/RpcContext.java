package cn.v5.lbrpc.common.client;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by yangwei on 15-5-27.
 */
public class RpcContext {
    public final static ThreadLocal<RpcContext> LOCAL = new ThreadLocal<RpcContext>() {
        @Override
        protected RpcContext initialValue() {
            return new RpcContext();
        }
    };

    ListenableFuture<?> result;

    public static RpcContext getContext() {
        return LOCAL.get();
    }

    public <T> ListenableFuture<T> getResult() {
        return (ListenableFuture<T>) result;
    }

    public void setResult(ListenableFuture<?> result) {
        this.result = result;
    }
}

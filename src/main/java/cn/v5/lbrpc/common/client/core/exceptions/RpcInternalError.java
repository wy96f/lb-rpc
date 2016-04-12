package cn.v5.lbrpc.common.client.core.exceptions;

/**
 * Created by yangwei on 15-5-4.
 */
public class RpcInternalError extends RpcException {
    private static final long serialVersionUID = 0;

    public RpcInternalError(String message) {
        super(message);
    }

    public RpcInternalError(String message, Throwable cause) {
        super(message, cause);
    }

    public RpcInternalError(Throwable cause) {
        super(cause);
    }
}

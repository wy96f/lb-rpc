package cn.v5.lbrpc.common.client.core.exceptions;

/**
 * Created by yangwei on 15-5-4.
 */
public class RpcException extends RuntimeException {
    private static final long serailVersionUID = 234151519241L;

    public RpcException() {
        super();
    }

    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }

    public RpcException(Throwable cause) {
        super(cause);
    }

    public RpcException copy() {
        return new RpcException(getMessage(), this);
    }
}

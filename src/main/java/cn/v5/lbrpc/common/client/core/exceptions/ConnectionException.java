package cn.v5.lbrpc.common.client.core.exceptions;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-5-5.
 */
public class ConnectionException extends Exception {
    private static final long serialVersionUID = 1941451791L;

    public final InetSocketAddress address;

    public ConnectionException(InetSocketAddress address, String message) {
        super(message);
        this.address = address;
    }

    public ConnectionException(InetSocketAddress address, Throwable cause, String message) {
        super(message, cause);
        this.address = address;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", address, super.getMessage());
    }
}

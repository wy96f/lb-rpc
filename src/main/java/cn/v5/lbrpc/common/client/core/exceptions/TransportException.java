package cn.v5.lbrpc.common.client.core.exceptions;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-5-5.
 */
public class TransportException extends ConnectionException {
    private static long serialVersionUID = 0;

    public TransportException(InetSocketAddress address, String message) {
        super(address, message);
    }

    public TransportException(InetSocketAddress address, Throwable cause, String message) {
        super(address, cause, message);
    }
}

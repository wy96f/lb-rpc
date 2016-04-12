package cn.v5.lbrpc.common.client.core.exceptions;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-5-7.
 */
public class OperationTimedOutException extends ConnectionException {
    public OperationTimedOutException(InetSocketAddress address) {
        super(address, "Operation timed out");
    }
}

package cn.v5.lbrpc.thrift.transport;

import org.apache.thrift.transport.TTransportException;

/**
 * Application level exception
 *
 */
public class THeaderException extends TTransportException {
    private static final long serialVersionUID = 1L;

    public THeaderException(String message) {
        super(message);
    }

    public THeaderException(Throwable throwable) {
        super(throwable);
    }
}
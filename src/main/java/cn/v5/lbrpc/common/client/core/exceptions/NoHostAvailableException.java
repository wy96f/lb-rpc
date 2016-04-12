package cn.v5.lbrpc.common.client.core.exceptions;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by yangwei on 15-5-4.
 */
public class NoHostAvailableException extends RpcException {
    private static final long serialVersionUID = 0;

    private final Map<InetSocketAddress, Throwable> errors;

    public NoHostAvailableException(Map<InetSocketAddress, Throwable> errors) {
        super(makeMessage(errors));
        this.errors = errors;
    }

    private static String makeMessage(Map<InetSocketAddress, Throwable> errors) {
        if (errors.size() == 0)
            return "All host(s) tried for query failed (no host was tried)";

        if (errors.size() <= 3) {
            StringBuilder sb = new StringBuilder();
            sb.append("All host(s) tried for query failed (tried: ");
            int n = 0;
            for (Map.Entry<InetSocketAddress, Throwable> entry : errors.entrySet()) {
                if (n++ > 0) sb.append(", ");
                sb.append(entry.getKey()).append(" (").append(entry.getValue()).append(')');
            }
            return sb.append(')').toString();
        }
        return String.format("All host(s) tried for query failed (tried: %s - use getErrors() for details)", errors.keySet());
    }
}

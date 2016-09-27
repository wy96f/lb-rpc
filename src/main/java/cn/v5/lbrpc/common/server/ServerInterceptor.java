package cn.v5.lbrpc.common.server;

import java.net.SocketAddress;
import java.util.Map;

/**
 * Created by yangwei on 26/9/16.
 */
public interface ServerInterceptor {
    public void preProcess(String fullMethod, SocketAddress address, Map<String, String> header);
    public void postProcess(Exception e);
}

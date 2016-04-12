package cn.v5.lbrpc.common.client.core.loadbalancer;


import com.google.common.util.concurrent.ListenableFuture;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-6-4.
 */
public interface ServiceRegistration {
    public void registerServer(String service, String proto, InetSocketAddress address);

    public ListenableFuture<?> unregisterServer(String service, String proto, InetSocketAddress address);
}
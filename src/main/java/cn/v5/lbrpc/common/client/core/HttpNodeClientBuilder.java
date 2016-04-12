package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.http.client.core.HttpNodeClient;

/**
 * Created by yangwei on 15-6-24.
 */
public class HttpNodeClientBuilder<T extends IRequest, V extends IResponse> extends AbstractNodeClientBuilder<T, V, HttpNodeClientBuilder<T, V>, AbstractNodeClient<T, V>> {
    @Override
    protected AbstractNodeClient<T, V> createClient(String proto) {
        HttpNodeClient nodeClient = new HttpNodeClient(getConfiguration(), serviceDiscoveryImpl);
        return nodeClient;
    }
}
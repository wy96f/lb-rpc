package cn.v5.lbrpc.http.client.core;

import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Configuration;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 15-6-8.
 */
public class HttpNodeClient<T extends IRequest, V extends IResponse> extends AbstractNodeClient<T, V> {
    public HttpNodeClient(Configuration configuration, ServiceDiscovery serviceDiscoveryImpl) {
        super(CBUtil.HTTP_PROTO, configuration, serviceDiscoveryImpl);
        this.manager = new HttpManager(configuration);
    }
}
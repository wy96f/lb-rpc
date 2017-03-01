package cn.v5.lbrpc.http.client.core;

import cn.v5.lbrpc.common.client.core.AbstractNodeClient;
import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.client.core.exceptions.NoHostAvailableException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.exceptions.RpcInternalError;
import cn.v5.lbrpc.common.client.core.loadbalancer.LoadBalancingPolicy;
import cn.v5.lbrpc.common.utils.Pair;
import com.google.common.base.Throwables;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Created by yangwei on 15-6-8.
 */
public class HttpRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(HttpRequestHandler.class);

    private final AbstractNodeClient httpNodeClient;

    private final HttpManager manager;

    private final RequestContext context;

    private volatile Map<InetSocketAddress, Throwable> errors;

    private List<Object> interceptors;


    public HttpRequestHandler(AbstractNodeClient nodeClient) {
        this.httpNodeClient = nodeClient;
        this.manager = (HttpManager) nodeClient.getManager();
        this.context = null;
    }

    public HttpRequestHandler(AbstractNodeClient nodeClient, List<Object> interceptors, RequestContext requestContext) {
        this.context = requestContext;
        this.httpNodeClient = nodeClient;
        this.interceptors = interceptors;
        this.manager = (HttpManager) nodeClient.getManager();
    }

    public Object sendRequest() {
        try {
            Iterator<Host> queryPlan = manager.getLoadBalancingPolicy(context.getServiceAndProto()).queryPlan(new HttpRequestAdapter(context));

            while (queryPlan.hasNext()) {
                Host host = queryPlan.next();
                if (logger.isTraceEnabled())
                    logger.trace("Querying node {}", host);
                Pair<Object, Boolean> res = query(context.getRequestClass(), host);
                if (!res.right) {
                    return res.left;
                }

                //httpNodeClient.forceQuickRefresh(context.getServiceName()).get();
            }
        } catch (RpcException e) {
            throw e;
        } catch (Exception e) {
            throw new RpcInternalError("An unexpected error happened while sending requests", e);
        }
        throw new NoHostAvailableException(errors == null ? Collections.<InetSocketAddress, Throwable>emptyMap() : errors);
    }

    /**
     * @return pair of result and retry flag.
     */
    private <T> Pair<Object, Boolean> query(Class<T> type, Host host) throws IllegalAccessException {
        ResteasyClient client = manager.clients.get(host);

        if (client == null || client.isClosed()) {
            return Pair.create(null, true);
        }

        try {
            // TODO cache target or proxy?
            ResteasyWebTarget target = client.target("http://" + host.getAddress().getHostString() + ":" + host.getAddress().getPort());
            if (interceptors != null && !interceptors.isEmpty()) {
                for (Object interceptor : interceptors) {
                    target.register(interceptor);
                }
            }
            T proxy = target.proxy(type);

            Method method = context.getRealMethod();
            return Pair.create(method.invoke(proxy, context.getParams()), false);
        } catch (IllegalStateException e) {
            logError(host.getAddress(), e);
            return Pair.create(null, true);
        } catch (InvocationTargetException e) {
            if (Throwables.getRootCause(e) instanceof IOException) {
                manager.triggerOnDown(host, false, true);
                logError(host.getAddress(), e.getCause());
                return Pair.create(null, true);
            } else if (Throwables.getRootCause(e) instanceof NotFoundException) {
                logError(host.getAddress(), e.getCause());
                return Pair.create(null, true);
            } else {
                logError(host.getAddress(), e.getCause());
                throw new RpcException(e.getCause());
            }
        }
    }

    private void logError(InetSocketAddress address, Throwable exception) {
        logger.debug("Error querying {}, trying next host (error is: {})", address, Throwables.getStackTraceAsString(exception));
        if (errors == null)
            errors = new HashMap<InetSocketAddress, Throwable>();
        errors.put(address, Throwables.getRootCause(exception));
    }

    public void init(Pair<String, String> serviceAndProto, List<InetSocketAddress> initContactPoints, LoadBalancingPolicy loadBalancingPolicy) {
        manager.addLoadBalancingPolicy(serviceAndProto, loadBalancingPolicy);
        manager.init(serviceAndProto, initContactPoints);
    }
}
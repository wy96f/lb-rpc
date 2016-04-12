package cn.v5.lbrpc.http.client.core;

import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.common.utils.Pair;

import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-6-8.
 */
public class RequestContext {
    Pair<String, String> serviceAndProto;

    Class<?> requestClass;

    Method realMethod;

    Object[] params;

    public RequestContext(String serviceName) {
        this.serviceAndProto = Pair.create(serviceName, CBUtil.HTTP_PROTO);
    }

    public RequestContext(String serviceName, Class<?> requestClass, Method realMethod, Object[] params) {
        this.serviceAndProto = Pair.create(serviceName, CBUtil.HTTP_PROTO);
        this.requestClass = requestClass;
        this.realMethod = realMethod;
        this.params = params;
    }

    public Pair<String, String> getServiceAndProto() {
        return serviceAndProto;
    }

    public Class<?> getRequestClass() {
        return requestClass;
    }

    public Object[] getParams() {
        return params;
    }

    public Method getRealMethod() {
        return realMethod;
    }
}
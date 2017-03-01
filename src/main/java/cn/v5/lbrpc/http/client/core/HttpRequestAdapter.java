package cn.v5.lbrpc.http.client.core;

import cn.v5.lbrpc.common.data.IRequest;

/**
 * Created by wy96fyw@gmail.com on 2017/2/4.
 */
public class HttpRequestAdapter implements IRequest {
    private final RequestContext context;

    public HttpRequestAdapter(RequestContext context) {
        this.context = context;
    }

    @Override
    public void setStreamId(int id) {

    }

    @Override
    public void setHeader(String key, String value) {

    }

    @Override
    public String getService() {
        return context.getServiceAndProto().left;
    }

    @Override
    public String getMethod() {
        return context.getRealMethod().getName();
    }

    @Override
    public Object[] getArgs() {
        return context.getParams();
    }
}

package cn.v5.lbrpc.http.utils;

import org.jboss.resteasy.util.GetRestful;

/**
 * Created by yangwei on 9/7/15.
 */
public class HttpUtils {
    public static String getServiceName(Class<?> serviceImpl) {
        Class restful = GetRestful.getRootResourceClass(serviceImpl);
        if (restful == null) {
            String msg = "Class is not a root resource.  It, or one of its interfaces must be annotated with @Path: " + serviceImpl.getName() + " implements: ";
            for (Class intf : serviceImpl.getInterfaces()) {
                msg += " " + intf.getName();
            }
            throw new RuntimeException(msg);
        }
        return restful.getSimpleName();
    }
}

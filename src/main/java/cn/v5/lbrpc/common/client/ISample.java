package cn.v5.lbrpc.common.client;

import java.lang.reflect.Method;

/**
 * Created by yangwei on 12/9/16.
 */
public interface ISample {
    public boolean sample(Object proxy, Method method, Object[] args);
}

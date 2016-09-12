package cn.v5.lbrpc.common.client;

import java.lang.reflect.Method;

/**
 * Created by yangwei on 12/9/16.
 */
public class AlwaysSample implements ISample {
    @Override
    public boolean sample(Object proxy, Method method, Object[] args) {
        return true;
    }
}

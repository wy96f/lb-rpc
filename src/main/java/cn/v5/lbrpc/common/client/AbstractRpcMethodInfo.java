package cn.v5.lbrpc.common.client;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yangwei on 15-5-4.
 */
public abstract class AbstractRpcMethodInfo {
    private Method method;
    private String serviceName;
    private String methodName;

    private Class<?>[] inputClasses;
    private Class<?> outputClass;
    // exceptions declared by method
    private Class<?>[] exceptionClasses;

    protected AbstractRpcMethodInfo(Method method) {
        this.method = method;

        this.inputClasses = method.getParameterTypes();

        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && returnType != Void.class) {
            outputClass = returnType;
        }

        exceptionClasses = method.getExceptionTypes();
    }

    public abstract Object outputDecode(byte[] output) throws IOException;

    public Method getMethod() {
        return method;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Class<?>[] getInputClasses() {
        return inputClasses;
    }

    public Class<?> getOutputClass() {
        return outputClass;
    }

    public Class<?>[] getExceptionClasses() {
        return exceptionClasses;
    }
}

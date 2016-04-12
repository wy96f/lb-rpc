package cn.v5.lbrpc.thrift.utils;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

/**
 * Created by yangwei on 15-6-18.
 */
public class ThriftUtils {
    private static final Logger logger = LoggerFactory.getLogger(ThriftUtils.class);

    public static ProcessorInfo getProcessor(Object serviceImpl) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Set<? extends Class<?>> supers = TypeToken.of(serviceImpl.getClass()).getTypes().interfaces().rawTypes();
        Class<?> genInterface = null;
        for (Class<?> s : supers) {
            if (s.getSimpleName().endsWith("Iface")) {
                genInterface = s;
                break;
            }
        }

        Preconditions.checkNotNull(genInterface, "service " + serviceImpl.getClass() + " not implement thrift gen interface");

        Class<?> outClass = genInterface.getEnclosingClass();

        Preconditions.checkNotNull(outClass, "class " + genInterface.getClass() + " not in the thrift gen class");

        String serviceName = outClass.getSimpleName();

        for (Class<?> hc : outClass.getClasses()) {
            if (Modifier.isPublic(hc.getModifiers()) && Modifier.isStatic(hc.getModifiers())
                    && hc.getSimpleName().compareTo("Processor") == 0
                    && TBaseProcessor.class.isAssignableFrom(hc)) {
                return new ProcessorInfo(serviceName, (TProcessor) hc.getConstructor(genInterface).newInstance(serviceImpl));
            }
        }

        throw new IllegalArgumentException("class " + outClass.getClass() + " not contains processor");
    }

    public static String getServiceName(Class<?> serviceInterface) {
        Preconditions.checkArgument(serviceInterface.getSimpleName().endsWith("Iface"), "service " + serviceInterface + " is not thrift gen interface");

        Class<?> outClass = serviceInterface.getEnclosingClass();

        Preconditions.checkNotNull(outClass, "class " + serviceInterface.getClass() + " not in the thrift gen class");

        return outClass.getSimpleName();
    }

    public static Class<?> getArgsClass(Method method) {
        Class<?> genInterface = method.getDeclaringClass();
        Class<?> outClass = genInterface.getEnclosingClass();

        Preconditions.checkNotNull(genInterface, "class " + genInterface.getClass() + " not in the thrift gen class");

        String resultClassName = method.getName() + "_args";
        for (Class<?> declaredClasses : outClass.getDeclaredClasses()) {
            if (declaredClasses.getSimpleName().compareTo(resultClassName) == 0) {
                return declaredClasses;
            }
        }
        throw new IllegalArgumentException(outClass.getCanonicalName() + " has not inner class " + resultClassName);
    }

    public static Class<?> getResultClass(Method method) {
        Class<?> genInterface = method.getDeclaringClass();
        Class<?> outClass = genInterface.getEnclosingClass();

        Preconditions.checkNotNull(genInterface, "class " + genInterface.getClass() + " not in the thrift gen class");

        String resultClassName = method.getName() + "_result";
        for (Class<?> declaredClasses : outClass.getDeclaredClasses()) {
            if (declaredClasses.getSimpleName().compareTo(resultClassName) == 0) {
                return declaredClasses;
            }
        }
        return null;
    }

    public static class ProcessorInfo {
        private final String serviceName;
        private final TProcessor processor;

        public ProcessorInfo(String serviceName, TProcessor processor) {
            this.serviceName = serviceName;
            this.processor = processor;
        }

        public String getServiceName() {
            return serviceName;
        }

        public TProcessor getProcessor() {
            return processor;
        }
    }
}

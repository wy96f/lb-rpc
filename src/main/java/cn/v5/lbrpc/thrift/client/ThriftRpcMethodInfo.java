package cn.v5.lbrpc.thrift.client;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.thrift.utils.ThriftUtils;
import com.google.common.base.Throwables;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by yangwei on 15-6-24.
 */
public class ThriftRpcMethodInfo extends AbstractRpcMethodInfo {
    private final Class<?> resultClass;
    private final Method readMethod;
    private Method successMethod;
    private Field[] fields;
    private Field success;

    public ThriftRpcMethodInfo(Method method) throws NoSuchMethodException, NoSuchFieldException {
        super(method);
        this.resultClass = ThriftUtils.getResultClass(getMethod());
        this.readMethod = resultClass.getMethod("read", TProtocol.class);
        if (getOutputClass() != null) {
            this.successMethod = resultClass.getMethod("isSetSuccess", new Class[0]);
            this.success = resultClass.getField("success");
            this.fields = resultClass.getDeclaredFields();
        }
    }

    @Override
    public Object outputDecode(byte[] output) throws IOException {
        TTransport transport = new TMemoryInputTransport(output);
        TProtocol protocol = new TMultiplexedProtocol(new TBinaryProtocol(transport), getServiceName());

        try {
            Object result = resultClass.newInstance();

            readMethod.invoke(result, protocol);

            protocol.readMessageEnd();

            // return null if method has no return value
            if (getOutputClass() == null) {
                return null;
            }

            boolean successful = (boolean) successMethod.invoke(result);
            if (successful) {
                return success.get(result);
            } else {
                Class<?>[] exceptionClasses = getExceptionClasses();
                for (Field field : fields) {
                    if (field.getName().compareTo("success") == 0) {
                        continue;
                    }
                    Class<?> fieldClass = field.getType();
                    for (Class<?> exceptionClass : exceptionClasses) {
                        // return declared exception, ThriftMessage.onResponse will handle it
                        if (exceptionClass.isAssignableFrom(fieldClass) && field.get(result) != null) {
                            return field.get(result);
                        }
                    }
                }
                /**
                 * Null return val is fine in an all-Java world, but would break if you have a C++ client
                 * calling your Java service. Since a thrift definition like this:
                 * StructType myMethod() Turns into a C++ definition of:
                 * void myMethod(StructType& ret) (or direct return for primitive types)
                 * see http://grokbase.com/t/thrift/user/0868e2qd3v/null-ret-val for details.
                 */
                throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, resultClass.getSimpleName() + " failed: unknown result");
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e.getCause());
        } catch (InvocationTargetException e) {
            if (Throwables.getRootCause(e) instanceof TException) {
                throw new IOException(e.getCause());
            }
            throw new RuntimeException(e.getCause());
        } catch (TException e) {
            throw new IOException(e);
        }
    }
}

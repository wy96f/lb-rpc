package cn.v5.lbrpc.thrift;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yangwei on 23/7/15.
 */
public class EchoServiceImpl implements Echo.Iface {
    public static String echoResponse = "echoResponse";

    @Override
    public String echoReturnString(String p1, String p2) throws TException {
        return p1 + p2;
    }

    @Override
    public void echoReturnVoid(double d1, int i2) throws TException {
        return;
    }

    @Override
    public List<String> echoWithoutParams() throws TException {
        return ImmutableList.of(echoResponse);
    }

    @Override
    public boolean echoSerializedException(ByteBuffer b) throws SerializedException {
        throw new SerializedException(ByteBuffer.wrap(echoResponse.getBytes()));
    }

    @Override
    public double echoRuntimeException(Set<Integer> s) throws TException {
        throw new RuntimeException(echoResponse);
    }

    @Override
    public String echoTimeout(Map<Double, String> m) throws TException {
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        return "et";
    }

    @Override
    public String echoReturnNull() throws TException {
        return null;
    }

    @Override
    public String echoWithNullParameter(List<String> l) throws TException {
        return l == null ? "null" : "not null";
    }
}

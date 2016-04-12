package cn.v5.lbrpc.thrift;

import org.apache.thrift.TException;

/**
 * Created by yangwei on 31/8/15.
 */
public class NonExistentServiceImpl implements  ThriftNonExistentService.Iface {
    @Override
    public String requestNonExistentService(String p1) throws TException {
        return "";
    }
}

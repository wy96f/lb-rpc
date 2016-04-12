package cn.v5.lbrpc.common.utils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by yangwei on 17/7/15.
 */
public class ConnectStringParser {
    public static List<InetSocketAddress> parseConnectString(String connectStr) {
        List<InetSocketAddress> addresses = Lists.newArrayList();
        Iterable<String> hostAndPorts = Splitter.on(",").trimResults().omitEmptyStrings().split(connectStr);
        for (String hostAndPort : hostAndPorts) {
            int idx = hostAndPort.indexOf(":");
            if (idx > 0 && idx < hostAndPort.length() - 1) {
                int port = Integer.parseInt(hostAndPort.substring(idx + 1));
                addresses.add(new InetSocketAddress(hostAndPort.substring(0, idx), port));
            }
        }

        return addresses;
    }
}

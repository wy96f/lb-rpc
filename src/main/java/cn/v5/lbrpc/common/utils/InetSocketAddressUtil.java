package cn.v5.lbrpc.common.utils;

import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-6-4.
 */
public class InetSocketAddressUtil {
    public static String serializeAddress(InetSocketAddress address) {
        return address.getAddress().getHostAddress() + ":" + address.getPort();
    }

    public static InetSocketAddress deserializeAddress(String addressString) {
        String[] hostAndPort = addressString.split(":");
        return new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }
}

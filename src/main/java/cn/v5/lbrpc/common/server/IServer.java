package cn.v5.lbrpc.common.server;

/**
 * Created by yangwei on 15-6-5.
 */
public interface IServer {
    void start();

    void register(Object instance, String contextPath) throws Exception;

    void unregister();

    void stop();

    String getProto();
}

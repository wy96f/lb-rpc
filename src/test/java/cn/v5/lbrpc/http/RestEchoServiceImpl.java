package cn.v5.lbrpc.http;

import java.io.IOException;

/**
 * Created by yangwei on 15-5-11.
 */
public class RestEchoServiceImpl implements RestEchoService {
    public static String hello = "hello";
    public static int num = 100;

    @Override
    public RestEchoInfo echoPost(RestEchoInfo info) {
        RestEchoInfo ret = new RestEchoInfo();
        ret.setMessage(hello + info.getMessage());
        ret.setX(info.getX() + num);
        return ret;
    }

    @Override
    public RestEchoInfo echoPut(RestEchoInfo info) {
        RestEchoInfo ret = new RestEchoInfo();
        ret.setMessage(hello + info.getMessage());
        ret.setX(info.getX() + num);
        return ret;
    }

    @Override
    public String echoGet(String x) {
        return hello + x;
    }

    @Override
    public RestEchoInfo echoPostIOException(RestEchoInfo info) throws IOException {
        throw new IOException(hello);
    }

    @Override
    public String echoGetRuntimeException(String x) {
        throw new RuntimeException(hello);
    }

    @Override
    public RestEchoInfo echoPutTimeout(RestEchoInfo info) {
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        return null;
    }

    @Override
    public RestEchoInfo echoReturnNull() {
        return null;
    }

    @Override
    public String echoWithNullParameter(RestEchoInfo info) {
        return info == null ? "null" : "not null";
    }

    @Override
    public void echoReturnVoid() {

    }
}
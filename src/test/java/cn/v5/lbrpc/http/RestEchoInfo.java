package cn.v5.lbrpc.http;

/**
 * Created by yangwei on 15-5-11.
 */
public class RestEchoInfo {
    String message;

    int x;

    public RestEchoInfo() {
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    @Override
    public String toString() {
        return "RestEchoInfo{" +
                "message='" + message + '\'' +
                ", x=" + x +
                '}';
    }
}

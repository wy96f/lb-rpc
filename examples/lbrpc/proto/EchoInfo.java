package proto;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

/**
 * Created by yangwei on 15-5-11.
 */
public class EchoInfo {
    @Protobuf(description = "Echo消息内容")
    String message;

    @Protobuf
    int x;

    public EchoInfo() {
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
        return "EchoInfo{" +
                "message='" + message + '\'' +
                ", x=" + x +
                '}';
    }
}

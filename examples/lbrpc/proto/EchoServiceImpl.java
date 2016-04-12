package proto;

import com.baidu.jprotobuf.pbrpc.ProtobufRPCService;

/**
 * Created by yangwei on 15-5-11.
 */
public class EchoServiceImpl {
    @ProtobufRPCService(serviceName = "echoService", methodName = "echo")
    public EchoInfo echo(EchoInfo info) {
        EchoInfo ret = new EchoInfo();
        if (info == null) {
            ret.setMessage("hello " + "null");
            ret.setX(0 + 100);
        } else {
            ret.setMessage("hello " + info.getMessage());
            ret.setX(info.getX() + 100);
        }
        //throw new RuntimeException("just test");
        //return ret;
        return ret;
    }
}
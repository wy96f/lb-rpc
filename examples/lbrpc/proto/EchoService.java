package proto;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;

/**
 * Created by yangwei on 15-5-11.
 */
public interface EchoService {
    @ProtobufRPC(serviceName = "echoService")
    EchoInfo echo(EchoInfo info);
}

package proto;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;

/**
 * Created by yangwei on 15-5-19.
 */
public interface EchoListService {
    @ProtobufRPC(serviceName = "echoListService")
    EchoListInfo echo(EchoListInfo info);
}

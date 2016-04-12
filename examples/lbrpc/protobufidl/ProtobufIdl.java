package protobufidl;

import cn.v5.lbrpc.protobuf.data.ProtobufMessage;
import com.baidu.bjf.remoting.protobuf.ProtobufIDLGenerator;

/**
 * Created by yangwei on 15-5-19.
 */
public class ProtobufIdl {
    public static void main(String[] args) {
        String code = ProtobufIDLGenerator.getIDL(ProtobufMessage.class);
        System.out.println(code);
    }
}
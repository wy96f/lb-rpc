package proto;

import com.baidu.jprotobuf.pbrpc.ProtobufRPCService;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by yangwei on 15-5-19.
 */
public class EchoListServiceImpl {
    @ProtobufRPCService(serviceName = "echoListService", methodName = "echo")
    public EchoListInfo doEcho(EchoListInfo infos) {
        List<EchoInfo> rets = Lists.newArrayList();
        for (EchoInfo info : infos.getInfos()) {
            EchoInfo ret = new EchoInfo();
            ret.setMessage("hello:" + info.getMessage());
            rets.add(ret);
            //throw new RuntimeException("just test");
        }
        EchoListInfo listInfo = new EchoListInfo();
        listInfo.setInfos(rets);
        return listInfo;
    }
}

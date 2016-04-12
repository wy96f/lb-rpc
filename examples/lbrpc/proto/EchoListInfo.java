package proto;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

import java.util.List;

/**
 * Created by yangwei on 15-5-19.
 */
public class EchoListInfo {
    @Protobuf
    List<EchoInfo> infos;

    public List<EchoInfo> getInfos() {
        return infos;
    }

    public void setInfos(List<EchoInfo> infos) {
        this.infos = infos;
    }

    @Override
    public String toString() {
        return "EchoListInfo{" +
                "infos=" + infos +
                '}';
    }
}

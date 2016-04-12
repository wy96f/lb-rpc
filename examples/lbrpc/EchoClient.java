import cn.v5.lbrpc.common.client.RpcProxyFactory;
import cn.v5.lbrpc.common.client.core.loadbalancer.EtcdServiceDiscovery;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceDiscovery;
import cn.v5.lbrpc.common.utils.CBUtil;
import com.google.common.collect.ImmutableList;
import proto.EchoInfo;
import proto.EchoListInfo;
import proto.EchoService;

/**
 * Created by yangwei on 15-5-11.
 */
public class EchoClient {
    public static void main(String[] args) throws Exception {
        ServiceDiscovery sd = new EtcdServiceDiscovery("http://127.0.0.1:4001/");

        EchoService es = RpcProxyFactory.createBuilder().withServiceDiscovery(sd).create(EchoService.class, CBUtil.PROTOBUF_PROTO);
        EchoService es1 = RpcProxyFactory.createBuilder().withServiceDiscovery(sd).create(EchoService.class, CBUtil.PROTOBUF_PROTO);
        //EchoListService esList = RpcProxyFactory.create(EchoListService.class, CBUtil.PROTOBUF_PROTO, sd);

        EchoInfo info = new EchoInfo();
        info.setMessage("from yw");

        EchoInfo info1 = new EchoInfo();
        info1.setMessage("from bll");

        EchoListInfo infos = new EchoListInfo();
        infos.setInfos(ImmutableList.of(info, info1));

        EchoInfo newInfo = new EchoInfo();

        while (true) {
            try {
                System.out.println(es.echo(info));
                System.out.println(es1.echo(null));
                //esList.echo(infos);
                //ListenableFuture<EchoListInfo> echoListInfoFuture = RpcContext.getContext().getResult();
                //echoListInfoFuture.get();
            } catch (Exception e) {
                System.out.println("echo exception: " + e.getMessage());
                Thread.sleep(1000);
            }
        }
    }
}
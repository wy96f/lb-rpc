import cn.v5.lbrpc.common.client.core.loadbalancer.EtcdServiceRegistration;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import cn.v5.lbrpc.common.server.CompositeServer;
import proto.EchoListServiceImpl;
import proto.EchoServiceImpl;

/**
 * Created by yangwei on 15-5-11.
 */
public class EchoServer {
    public static void main(String[] args) throws Exception {
        int port = 50052;
        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }

        CompositeServer server = new CompositeServer(new EtcdServiceRegistration("http://127.0.0.1:4001"));

        EchoServiceImpl echoService = new EchoServiceImpl();
        EchoListServiceImpl echoListService = new EchoListServiceImpl();


        server.register(echoService, AbstractServerFactory.PROTOBUF_RPC, port);
        server.register(echoListService, AbstractServerFactory.PROTOBUF_RPC, port);

        System.out.println("started");
    }
}

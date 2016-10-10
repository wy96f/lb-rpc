package cn.v5.lbrpc.protobuf.client.core;

import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.IPipelineAndHeartbeat;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.protobuf.data.ProtobufFrame;
import cn.v5.lbrpc.protobuf.data.ProtobufMessage;
import cn.v5.lbrpc.protobuf.data.ProtobufRequestMeta;
import cn.v5.lbrpc.protobuf.utils.ErrCodes;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangwei on 15-6-19.
 */
public class ProtobufPipelineConfigurator implements IPipelineAndHeartbeat<ProtobufMessage, ProtobufMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufPipelineConfigurator.class);

    private static final ProtobufMessage.Encoder messageEncoder = new ProtobufMessage.Encoder();
    private static final ProtobufMessage.Decoder messageDecoder = new ProtobufMessage.Decoder();
    private static final ProtobufFrame.Encoder frameEncoder = new ProtobufFrame.Encoder();

    private static final Connection.ResponseCallback<ProtobufMessage, ProtobufMessage> HEARTBEAT_CALLBACK = new Connection.ResponseCallback<ProtobufMessage, ProtobufMessage>() {

        @Override
        public void sendRequest(RequestHandler.ResultSetCallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProtobufMessage request() {
            return ProtobufMessage.HEART_BEAT;
        }

        @Override
        public int retryCount() {
            return 0;
        }

        @Override
        public void onSet(Connection connection, ProtobufMessage response, int retryCount) {
            if (response.getResponse().getErrorCode() == ErrCodes.ST_HEARBEAT &&
                    ProtobufRequestMeta.HEARTBEAT_SERVICE.compareTo(response.getResponse().getErrorText()) == 0) {
                logger.debug("{} heartbeat query succeeded", connection);
            } else {
                fail(connection, new ConnectionException(connection.address, "Unexpected heartbeat response :" + response));
            }
        }

        @Override
        public void onException(Connection connection, Exception exception, int retryCount) {
            // Nothing to do: the connection is already defunct if we arrive here
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean onTimeout(Connection connection, int streamId, int retryCount) {
            fail(connection, new ConnectionException(connection.address, "Heartbeat query timedout"));
            return true;
        }

        private void fail(Connection connection, Exception e) {
            connection.defunct(e);
        }
    };

    @Override
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("frameDecoder", new ProtobufFrame.Decoder());
        pipeline.addLast("frameEncoder", frameEncoder);

        pipeline.addLast("messageEncoder", messageEncoder);
        pipeline.addLast("messageDecoder", messageDecoder);
    }

    @Override
    public Connection.ResponseCallback getHeartbeat() {
        return HEARTBEAT_CALLBACK;
    }
}

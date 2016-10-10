package cn.v5.lbrpc.thrift.client.core;

import cn.v5.lbrpc.common.client.AbstractRpcMethodInfo;
import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.DefaultResultFuture;
import cn.v5.lbrpc.common.client.core.IPipelineAndHeartbeat;
import cn.v5.lbrpc.common.client.core.RequestHandler;
import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.client.core.loadbalancer.RoundRobinPolicy;
import cn.v5.lbrpc.thrift.client.ThriftRpcMethodInfo;
import cn.v5.lbrpc.thrift.client.ThriftRpcProxy;
import cn.v5.lbrpc.thrift.data.HeartbeatInternal;
import cn.v5.lbrpc.thrift.data.ThriftFrame;
import cn.v5.lbrpc.thrift.data.ThriftMessage;
import com.github.kristofa.brave.http.BraveHttpHeaders;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * Created by yangwei on 15-6-19.
 */
public class ThriftPipelineConfigurator implements IPipelineAndHeartbeat<ThriftMessage, ThriftMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ThriftPipelineConfigurator.class);

    private static final ThriftFrame.Encoder frameEncoder = new ThriftFrame.Encoder();
    private static final ThriftMessage.Encoder messageEncoder = new ThriftMessage.Encoder();
    private static final ThriftMessage.Decoder messageDecoder = new ThriftMessage.Decoder();

    public static AbstractRpcMethodInfo HEART_BEAT_METHOD = null;

    public static ThriftMessage HEART_BEAT = null;
    private static final Connection.ResponseCallback<ThriftMessage, ThriftMessage> HEARTBEAT_CALLBACK = new Connection.ResponseCallback<ThriftMessage, ThriftMessage>() {
        @Override
        public void sendRequest(RequestHandler.ResultSetCallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int retryCount() {
            return 0;
        }

        @Override
        public ThriftMessage request() {
            return HEART_BEAT;
        }

        @Override
        public void onSet(Connection connection, ThriftMessage response, int retryCount) {
            DefaultResultFuture<HeartbeatInternal.Iface, ThriftMessage, ThriftMessage> resultFuture = new DefaultResultFuture<>(HEART_BEAT, HEART_BEAT_METHOD);
            try {
                Exception exceptionToReport = null;
                response.onResponse(connection, resultFuture);
            } catch (IOException e) {
                logger.error("Heartbeat response err: {}", e);
                fail(connection, new ConnectionException(connection.address, "Unexpected heartbeat response :" + response));
                return;
            }

            Futures.addCallback(resultFuture, new FutureCallback<HeartbeatInternal.Iface>() {
                @Override
                public void onSuccess(HeartbeatInternal.Iface result) {
                    logger.debug("{} heartbeat query succeeded", connection);
                }

                @Override
                public void onFailure(Throwable t) {
                    fail(connection, new ConnectionException(connection.address, "Unexpected heartbeat response :" + response));
                }
            });
        }

        @Override
        public void onException(Connection connection, Exception exception, int retryCount) {
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

    static {
        ThriftRpcProxy<HeartbeatInternal.Iface> thriftRpcProxy = new ThriftRpcProxy<>(null, HeartbeatInternal.Iface.class, new RoundRobinPolicy());

        AbstractRpcMethodInfo methodInfo = getHeartbeatMethod(thriftRpcProxy);
        if (methodInfo != null) {
            HEART_BEAT_METHOD = methodInfo;
            try {
                HEART_BEAT = thriftRpcProxy.makeRequestMessage(methodInfo, null);
                HEART_BEAT.setHeader(BraveHttpHeaders.Sampled.getName(), "0");
            } catch (IOException e) {
                logger.error("init heart_beat message err: {}", e);
            }
        }
    }

    public static AbstractRpcMethodInfo getHeartbeatMethod(ThriftRpcProxy<HeartbeatInternal.Iface> thriftRpcProxy) {
        AbstractRpcMethodInfo methodInfo = null;
        try {
            methodInfo = new ThriftRpcMethodInfo(HeartbeatInternal.Iface.class.getDeclaredMethod("heartbeat", new Class<?>[0]));
        } catch (NoSuchMethodException e) {
            logger.error("get heartbeat method err: {}", e);
            return methodInfo;
        }
        methodInfo.setServiceName(thriftRpcProxy.getServiceAndProto().left);
        methodInfo.setMethodName("heartbeat");

        return methodInfo;
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("frameDecoder", new ThriftFrame.Decoder(102400, 0, 4));
        pipeline.addLast("frameEncoder", frameEncoder);

        pipeline.addLast("messageEncoder", messageEncoder);
        pipeline.addLast("messageDecoder", messageDecoder);
    }

    @Override
    public Connection.ResponseCallback getHeartbeat() {
        if (HEARTBEAT_CALLBACK.request() != null) {
            return HEARTBEAT_CALLBACK;
        } else {
            return null;
        }
    }
}

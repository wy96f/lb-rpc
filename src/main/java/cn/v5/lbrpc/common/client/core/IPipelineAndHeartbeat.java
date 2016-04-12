package cn.v5.lbrpc.common.client.core;

import cn.v5.lbrpc.common.data.IRequest;
import cn.v5.lbrpc.common.data.IResponse;
import io.netty.channel.ChannelPipeline;

/**
 * Created by yangwei on 15-6-19.
 */
public interface IPipelineAndHeartbeat<T extends IRequest, V extends IResponse> {
    public void configurePipeline(ChannelPipeline pipeline);

    Connection.ResponseCallback<T, V> getHeartbeat();
}
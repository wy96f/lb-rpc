package cn.v5.lbrpc.common.data;

import cn.v5.lbrpc.common.client.core.Connection;
import cn.v5.lbrpc.common.client.core.DefaultResultFuture;

import java.io.IOException;

/**
 * Created by yangwei on 15-6-19.
 */
public interface IResponse {
    public int getStreamId();

    /**
     * @param connection
     * @param resultFuture
     * @return whether or not retry
     * @throws IOException
     */
    public boolean onResponse(Connection connection, DefaultResultFuture resultFuture) throws IOException;
}

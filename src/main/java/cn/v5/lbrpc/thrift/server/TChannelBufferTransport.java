package cn.v5.lbrpc.thrift.server;

import cn.v5.lbrpc.common.utils.CBUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by yangwei on 15-6-16.
 */
public class TChannelBufferTransport extends TTransport {
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 1024;
    public final ByteBuf in;
    public final ByteBuf out;
    private final Channel channel;
    private final int initialReaderIndex;

    public TChannelBufferTransport(ByteBuf in, Channel channel) {
        this.in = in;
        this.channel = channel;
        this.out = CBUtil.allocator.buffer(DEFAULT_OUTPUT_BUFFER_SIZE);

        this.initialReaderIndex = in.readerIndex();
    }

    public ByteBuf getOutputBuffer() {
        return out;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void open() throws TTransportException {

    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws TTransportException {
        int read = Math.min(in.readableBytes(), length);
        in.readBytes(bytes, offset, read);
        return read;
    }

    @Override
    public int readAll(byte[] bytes, int offset, int length) throws TTransportException {
        if (read(bytes, offset, length) < length) {
            throw new TTransportException("Buffer doesn't have enough bytes to read");
        }
        return length;
    }

    @Override
    public void write(byte[] bytes, int i, int i2) throws TTransportException {
        out.writeBytes(bytes, i, i2);
    }
}

package cn.v5.lbrpc.thrift.data;

import cn.v5.lbrpc.common.utils.CBUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * Created by yangwei on 15-6-16.
 */
public class ThriftFrame {
    public final ByteBuf body;

    public ThriftFrame(ByteBuf body) {
        this.body = body;
    }

    public boolean release() {
        return body.release();
    }

    public static class Decoder extends LengthFieldBasedFrameDecoder {

        public Decoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
            super(maxFrameLength, lengthFieldOffset, lengthFieldLength, 0, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            return new ThriftFrame(frame);
        }
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<ThriftFrame> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ThriftFrame msg, List<Object> out) throws Exception {
            ByteBuf header = CBUtil.allocator.buffer(4);
            header.writeInt(msg.body.readableBytes());
            out.add(header);
            out.add(msg.body);
        }
    }
}

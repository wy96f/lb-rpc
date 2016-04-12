package cn.v5.lbrpc.protobuf.data;

import cn.v5.lbrpc.common.utils.CBUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * Created by yangwei on 15-5-4.
 */
public class ProtobufFrame {
    public static final int VERSION_1 = 1;
    public static final int CURRENT_VERSION = VERSION_1;

    public final Header header;
    public final ByteBuf body;

    public ProtobufFrame(Header header, ByteBuf body) {
        this.header = header;
        this.body = body;
    }

    public static ProtobufFrame create(ByteBuf fullFrame) {
        int version = fullFrame.readByte();
        assert version == VERSION_1 : String.format("frame version %d not matched", version);
        int length = fullFrame.readInt();

        Header header = new Header(version, length);
        return new ProtobufFrame(header, fullFrame);
    }

    public boolean release() {
        return body.release();
    }

    public static class Decoder extends LengthFieldBasedFrameDecoder {
        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB

        public Decoder() {
            super(MAX_FRAME_LENTH, 1, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            return ProtobufFrame.create(frame);
        }
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<ProtobufFrame> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ProtobufFrame frame, List<Object> out) throws Exception {
            int headerLength = Header.HEADER_SIZE;
            ByteBuf header = CBUtil.allocator.buffer(headerLength);

            header.writeByte(CURRENT_VERSION);
            header.writeInt(frame.body.readableBytes());

            out.add(header);
            out.add(frame.body);
        }
    }

    public static class Header {
        public static final int HEADER_SIZE = 5;
        public final int version;

        public int len;

        public Header(int version, int len) {
            this.version = version;
            this.len = len;
        }

        public int getLen() {
            return len;
        }

        public void setLen(int len) {
            this.len = len;
        }
    }
}

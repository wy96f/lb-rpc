package cn.v5.lbrpc.thrift.data;

import cn.v5.lbrpc.common.utils.CBUtil;
import cn.v5.lbrpc.thrift.transport.CustomTFramedTransport;
import cn.v5.lbrpc.thrift.transport.THeaderException;
import cn.v5.lbrpc.thrift.transport.THeaderTransport;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangwei on 15-6-16.
 */
public class ThriftFrame {
    public final ByteBuf body;
    public final Map<String, String> header;

    public ThriftFrame(ByteBuf body, Map<String, String> header) {
        this.body = body;
        this.header = header;
    }

    public Map<String, String> getHeader() {
        return header;
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
            try {
                int l = frame.readableBytes();
                byte[] i32buf = new byte[4];
                frame.readBytes(i32buf);
                int version = CustomTFramedTransport.decodeWord(i32buf);
                if ((version & THeaderTransport.VERSION_MASK) == THeaderTransport.VERSION_1) {
                    ByteBuf body = CBUtil.allocator.buffer(l);
                    body.writeBytes(i32buf);
                    body.writeBytes(frame);
                    return new ThriftFrame(body, new HashMap<>());
                } else if ((version & THeaderTransport.HEADER_MAGIC_MASK) == THeaderTransport.HEADER_MAGIC) {
                    if (l - 4 < 10) {
                        throw new TTransportException("Header transport frame is too small");
                    }
                    byte[] buff = new byte[l];
                    System.arraycopy(i32buf, 0, buff, 0, 4);
                    frame.readBytes(buff, 4, l - 4);
                    int flags = version & THeaderTransport.HEADER_FLAGS_MASK;
                    int seqId = THeaderTransport.decodeWord(buff, 4);

                    int headerSize = THeaderTransport.decodeShort(buff, 8);
                    ByteBuffer frameBuffer = ByteBuffer.wrap(buff);
                    frameBuffer.position(10);

                    headerSize = headerSize * 4;
                    int endHeader = headerSize + frameBuffer.position();
                    if (headerSize > frameBuffer.remaining()) {
                        throw new TTransportException("Header size is larger than frame buffer");
                    }
                    int protoId = THeaderTransport.readVarint32Buf(frameBuffer);
                    int numHeaders = THeaderTransport.readVarint32Buf(frameBuffer);


                    if (numHeaders != 0) {
                        throw new TTransportException("Transform data not supported");
                    }
                    if (protoId != THeaderTransport.T_BINARY_PROTOCOL) {
                        throw new TTransportException("Trying to recv non binary data");

                    }
                    int hmacSz = 0;
                    Map<String, String> readHeaders = new HashMap<>();
                    Map<String, String> readPersistentHeaders = new HashMap<>();

                    while (frameBuffer.position() < endHeader) {
                        int infoId = THeaderTransport.readVarint32Buf(frameBuffer);
                        if (infoId == THeaderTransport.Infos.INFO_KEYVALUE.getValue()) {
                            int numKeys = THeaderTransport.readVarint32Buf(frameBuffer);
                            for (int i = 0; i < numKeys; i++) {
                                String key = THeaderTransport.readString(frameBuffer);
                                String value = THeaderTransport.readString(frameBuffer);
                                readHeaders.put(key, value);
                            }
                        } else if (infoId == THeaderTransport.Infos.INFO_PKEYVALUE.getValue()) {
                            int numKeys = THeaderTransport.readVarint32Buf(frameBuffer);
                            for (int i = 0; i < numKeys; i++) {
                                String key = THeaderTransport.readString(frameBuffer);
                                String value = THeaderTransport.readString(frameBuffer);
                                readPersistentHeaders.put(key, value);
                            }
                        } else {
                            // Unknown info ID, continue on to reading data.
                            break;
                        }
                    }
                    readHeaders.putAll(readPersistentHeaders);

                    // Read in the data section.
                    frameBuffer.position(endHeader);
                    frameBuffer.limit(frameBuffer.limit() - hmacSz); // limit to data without mac

                    ByteBuf body = CBUtil.allocator.buffer(frameBuffer.remaining());
                    body.writeBytes(frameBuffer);
                    return new ThriftFrame(body, readHeaders);
                } else {
                    throw new THeaderException("Protocol wrong");
                }
            } finally {
                frame.release();
            }
        }
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<ThriftFrame> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ThriftFrame msg, List<Object> out) throws Exception {
            if (msg.header == null || msg.header.isEmpty()) {
                ByteBuf header = CBUtil.allocator.buffer(4);
                header.writeInt(msg.body.readableBytes());
                out.add(header);
                out.add(msg.body);
            } else {
                ByteBuffer infoData2 = THeaderTransport.flushInfoHeaders(
                        THeaderTransport.Infos.INFO_KEYVALUE, msg.header);

                ByteBuffer headerData = ByteBuffer.allocate(10);
                THeaderTransport.writeVarint(headerData, THeaderTransport.T_BINARY_PROTOCOL);
                THeaderTransport.writeVarint(headerData, 0);
                headerData.limit(headerData.position());
                headerData.position(0);

                int headerSize = 0 + 0 +
                        infoData2.remaining() + headerData.remaining();
                int paddingSize = 4 - headerSize % 4;
                headerSize += paddingSize;

                // Allocate buffer for the headers.
                // 14 bytes for sz, magic , flags , seqId , headerSize
                ByteBuf prefix = CBUtil.allocator.buffer(headerSize + 14);

                // See thrift/doc/HeaderFormat.txt for more info on wire format
                int bodyLen = msg.body.readableBytes();
                prefix.writeInt(10 + headerSize + bodyLen); // size
                prefix.writeShort((short) (THeaderTransport.HEADER_MAGIC >> 16)); // magic
                prefix.writeShort((short) 0); // flags
                prefix.writeInt(0); // seqId
                prefix.writeShort((short) (headerSize / 4)); // headerSize

                prefix.writeBytes(headerData);
                prefix.writeBytes(infoData2);

                // There are no info headers for this version
                // Pad out the header with 0x00
                for (int i = 0; i < paddingSize; i++) {
                    prefix.writeByte((byte)0x00);
                }

                out.add(prefix);
                out.add(msg.body);
            }

        }
    }
}

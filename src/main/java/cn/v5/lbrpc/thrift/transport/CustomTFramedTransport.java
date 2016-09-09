package cn.v5.lbrpc.thrift.transport;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Created by yangwei on 9/9/16.
 */
public class CustomTFramedTransport extends TTransport {
    protected static final int DEFAULT_MAX_LENGTH = 16384000;

    private int maxLength_;

    /**
     * Underlying transport
     */
    protected TTransport transport_ = null;

    /**
     * Buffer for output
     */
    protected final TByteArrayOutputStream writeBuffer_ =
            new TByteArrayOutputStream(1024);

    /**
     * Buffer for input
     */
    protected TMemoryInputTransport readBuffer_ = new TMemoryInputTransport(new byte[0]);

    public static class Factory extends TTransportFactory {
        private int maxLength_;

        public Factory() {
            maxLength_ = CustomTFramedTransport.DEFAULT_MAX_LENGTH;
        }

        public Factory(int maxLength) {
            maxLength_ = maxLength;
        }

        @Override
        public TTransport getTransport(TTransport base) {
            return new CustomTFramedTransport(base, maxLength_);
        }
    }

    /**
     * Constructor wraps around another transport
     */
    public CustomTFramedTransport(TTransport transport, int maxLength) {
        transport_ = transport;
        maxLength_ = maxLength;
    }

    public CustomTFramedTransport(TTransport transport) {
        transport_ = transport;
        maxLength_ = CustomTFramedTransport.DEFAULT_MAX_LENGTH;
    }

    public void open() throws TTransportException {
        transport_.open();
    }

    public boolean isOpen() {
        return transport_.isOpen();
    }

    public void close() {
        transport_.close();
    }

    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (readBuffer_ != null) {
            int got = readBuffer_.read(buf, off, len);
            if (got > 0) {
                return got;
            }
        }

        // Read another frame of data
        readFrame();

        return readBuffer_.read(buf, off, len);
    }

    @Override
    public byte[] getBuffer() {
        return readBuffer_.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return readBuffer_.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return readBuffer_.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        readBuffer_.consumeBuffer(len);
    }

    private final byte[] i32buf = new byte[4];

    protected void readFrame() throws TTransportException {
        transport_.readAll(i32buf, 0, 4);
        int size = decodeWord(i32buf);

        if (size < 0) {
            throw new TTransportException("Read a negative frame size (" + size + ")!");
        }

        if (size > maxLength_) {
            throw new TTransportException("Frame size (" + size + ") larger than max length (" + maxLength_ + ")!");
        }

        byte[] buff = new byte[size];
        transport_.readAll(buff, 0, size);
        readBuffer_.reset(buff);
    }

    public void write(byte[] buf, int off, int len) throws TTransportException {
        writeBuffer_.write(buf, off, len);
    }

    @Override
    public void flush() throws TTransportException {
        byte[] buf = writeBuffer_.get();
        int len = writeBuffer_.len();
        writeBuffer_.reset();

        encodeWord(len, i32buf);
        transport_.write(i32buf, 0, 4);
        transport_.write(buf, 0, len);
        transport_.flush();
    }

    /**
     * Functions to encode/decode int32 and int16 to/from network byte order
     */
    public static final void encodeWord(final int frameSize,
                                        final byte[] buf) {
        buf[0] = (byte)(0xff & (frameSize >> 24));
        buf[1] = (byte)(0xff & (frameSize >> 16));
        buf[2] = (byte)(0xff & (frameSize >> 8));
        buf[3] = (byte)(0xff & (frameSize));
    }

    public static final int decodeWord(final byte[] buf) {
        return decodeWord(buf, 0);
    }

    public static final int decodeWord(final byte[] buf, int off) {
        return
                ((buf[0 + off] & 0xff) << 24) |
                        ((buf[1 + off] & 0xff) << 16) |
                        ((buf[2 + off] & 0xff) <<  8) |
                        ((buf[3 + off] & 0xff));
    }

    public static final short decodeShort(final byte[] buf) {
        return decodeShort(buf, 0);
    }

    public static final short decodeShort(final byte[] buf, int off) {
        return (short)(
                ((buf[0 + off] & 0xff) << 8) |
                        ((buf[1 + off] & 0xff))
        );
    }

    public static final void encodeShort(final int value,
                                         final byte[] buf) {
        buf[0] = (byte)(0xff & (value >> 8));
        buf[1] = (byte)(0xff & (value));
    }
}

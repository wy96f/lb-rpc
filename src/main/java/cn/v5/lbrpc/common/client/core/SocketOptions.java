package cn.v5.lbrpc.common.client.core;

/**
 * Created by yangwei on 15-5-26.
 */
public class SocketOptions {
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 3000;
    public static final int DEFAULT_READ_TIMEOUT_MILLIS = 12000;

    private int connectionTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;

    private int readTimeoutMillis = DEFAULT_READ_TIMEOUT_MILLIS;

    private Boolean keepAlive;

    private Boolean reuseAddress;

    private Integer soLinger;

    private Boolean tcpNoDelay;

    private Integer sendBufferSize;

    private Integer recvBufferSize;

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public SocketOptions setConnectionTimeoutMillis(int connectionTimeoutMillis) {
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        return this;
    }

    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public SocketOptions setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    public Boolean isKeepAlive() {
        return keepAlive;
    }

    public SocketOptions setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public Boolean isReuseAddress() {
        return reuseAddress;
    }

    public SocketOptions setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    public Integer getSoLinger() {
        return soLinger;
    }

    public SocketOptions setSoLinger(Integer soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    public Boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public SocketOptions setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    public SocketOptions setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    public Integer getRecvBufferSize() {
        return recvBufferSize;
    }

    public SocketOptions setRecvBufferSize(int recvBufferSize) {
        this.recvBufferSize = recvBufferSize;
        return this;
    }
}

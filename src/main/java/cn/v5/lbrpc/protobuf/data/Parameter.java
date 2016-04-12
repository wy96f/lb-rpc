package cn.v5.lbrpc.protobuf.data;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

/**
 * Created by yangwei on 15-5-20.
 */
public class Parameter {
    @Protobuf
    private int pi;
    @Protobuf
    private double pd;
    @Protobuf
    private float pf;
    @Protobuf
    private long pl;
    @Protobuf
    private byte[] pb;
}

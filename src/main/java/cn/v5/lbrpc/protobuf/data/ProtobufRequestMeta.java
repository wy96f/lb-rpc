/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.v5.lbrpc.protobuf.data;

import cn.v5.lbrpc.common.data.Writerable;
import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

import java.io.IOException;

public class ProtobufRequestMeta implements cn.v5.lbrpc.common.data.Readable, Writerable {
    public static final String HEARTBEAT_SERVICE = "~";
    public static final String HEARTBEAT_METHOD = "@";

    private static final Codec<ProtobufRequestMeta> CODEC = ProtobufProxy.create(ProtobufRequestMeta.class);

    @Protobuf(required = true)
    private String serviceName;

    @Protobuf(required = true)
    private String methodName;

    @Protobuf
    private Long logId;

    public ProtobufRequestMeta() {
    }

    public ProtobufRequestMeta(String serviceName, String methodName) {
        this.serviceName = serviceName;
        this.methodName = methodName;
    }

    public String getSerivceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Long getLogId() {
        return logId;
    }

    public void setLogId(Long logId) {
        this.logId = logId;
    }

    public void read(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("param 'bytes' is null.");
        }

        try {
            ProtobufRequestMeta meta = CODEC.decode(bytes);
            copy(meta);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void copy(ProtobufRequestMeta meta) {
        if (meta == null) {
            return;
        }
        setLogId(meta.getLogId());
        setMethodName(meta.getMethodName());
        setServiceName(meta.getSerivceName());
    }

    public byte[] write() {
        try {
            return CODEC.encode(this);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public ProtobufRequestMeta copy() {
        ProtobufRequestMeta protobufRequestMeta = new ProtobufRequestMeta();
        protobufRequestMeta.copy(this);
        return protobufRequestMeta;
    }
}

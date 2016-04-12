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

public class ProtobufResponseMeta implements cn.v5.lbrpc.common.data.Readable, Writerable {

    private static final Codec<ProtobufResponseMeta> CODEC = ProtobufProxy.create(ProtobufResponseMeta.class);
    @Protobuf(required = true)
    private Integer errorCode;
    @Protobuf
    private String errorText;

    public ProtobufResponseMeta() {
    }

    public ProtobufResponseMeta(Integer errorCode, String errorText) {
        super();
        this.errorCode = errorCode;
        this.errorText = errorText;
    }

    public Integer getErrorCode() {
        if (errorCode == null) {
            errorCode = 0;
        }
        return errorCode;
    }

    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorText() {
        return errorText;
    }

    public void setErrorText(String errorText) {
        this.errorText = errorText;
    }

    public byte[] write() {
        try {
            return CODEC.encode(this);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void copy(ProtobufResponseMeta meta) {
        if (meta == null) {
            return;
        }
        setErrorCode(meta.getErrorCode());
        setErrorText(meta.getErrorText());
    }

    public void read(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("param 'bytes' is null.");
        }

        try {
            ProtobufResponseMeta meta = CODEC.decode(bytes);
            copy(meta);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public ProtobufResponseMeta copy() {
        ProtobufResponseMeta protobufResponseMeta = new ProtobufResponseMeta();
        protobufResponseMeta.copy(this);
        return protobufResponseMeta;
    }


}

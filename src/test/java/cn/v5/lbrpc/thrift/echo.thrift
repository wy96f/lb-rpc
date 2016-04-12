namespace java cn.v5.lbrpc.thrift

exception SerializedException {
    1: required binary payload
}

exception DeserializedException {
    1: required binary payload
}

service Echo {
    string echoReturnString(1:string p1, 2:string p2)

    void echoReturnVoid(1:double d1, 2:i32 i2)

    list<string> echoWithoutParams()

    bool echoSerializedException(1:binary b) throws (1:SerializedException serializedException, 2:DeserializedException deserializedException)

    double echoRuntimeException(1:set<i32> s)

    string echoTimeout(1:map<double, string> m)

    string echoReturnNull()

    string echoWithNullParameter(1:list<string> l)
}
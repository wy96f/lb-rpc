package cn.v5.lbrpc.protobuf.utils;

/**
 * Created by yangwei on 15-5-5.
 */
public class ErrCodes {
    /**
     * success status
     */
    public static final int ST_HEARBEAT = -219357864;

    public static final int ST_SUCCESS = 0;

    public static final int ST_ERROR = 2001;

    public static final int ST_SERVICE_NOTFOUND = 1001;


    public static final String MSG_SERVICE_NOTFOUND = "service %s not found";

    public static boolean isSuccess(int errorCode) {
        return ST_SUCCESS == errorCode;
    }
}

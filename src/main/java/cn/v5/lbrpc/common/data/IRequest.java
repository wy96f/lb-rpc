package cn.v5.lbrpc.common.data;

/**
 * Created by yangwei on 15-6-19.
 */
public interface IRequest {
    public void setStreamId(int id);
    public void setHeader(String key, String value);
    public String getService();
    public String getMethod();
    public Object[] getArgs();
}
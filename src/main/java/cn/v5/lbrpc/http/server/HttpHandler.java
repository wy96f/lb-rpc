package cn.v5.lbrpc.http.server;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by yangwei on 15-6-5.
 */
public interface HttpHandler {
    void handle(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException;
}

package cn.v5.lbrpc.http.server;

import com.google.common.collect.Maps;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by yangwei on 15-6-6.
 */
public class DispatcherServlet extends HttpServlet {
    private static final DispatcherServlet instance = new DispatcherServlet();

    private static final Map<Integer, HttpHandler> handlers = Maps.newConcurrentMap();

    public static DispatcherServlet getInstance() {
        return instance;
    }

    public static void addHttpHandler(int port, HttpHandler processor) {
        handlers.put(port, processor);
    }

    public static void removeHttpHandler(int port) {
        handlers.remove(port);
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HttpHandler handler = handlers.get(req.getLocalPort());
        if (handler == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Service not found.");
        } else {
            handler.handle(req, resp);
        }
    }
}
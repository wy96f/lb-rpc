package cn.v5.lbrpc.http.server;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by yangwei on 15-6-5.
 */
public class TomcatHttpServer extends AbstractHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(TomcatHttpServer.class);

    private final Tomcat tomcat;

    private final InetSocketAddress address;

    public TomcatHttpServer(InetSocketAddress address, final HttpHandler handler) {
        this.address = address;
        DispatcherServlet.addHttpHandler(address.getPort(), handler);
        String baseDir = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
        tomcat = new Tomcat();
        tomcat.setBaseDir(baseDir);
        tomcat.setPort(address.getPort());
        tomcat.getConnector().setProperty(
                "maxThreads", String.valueOf(Runtime.getRuntime().availableProcessors() * 4));
        //tomcat.getConnector().setProperty("socket.soReuseAddress", String.valueOf(true));
//        tomcat.getConnector().setProperty(
//                "minSpareThreads", String.valueOf(url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS)));

        //tomcat.getConnector().setProperty(
        //        "maxConnections", String.valueOf(url.getParameter(Constants.ACCEPTS_KEY, -1)));

        tomcat.getConnector().setProperty("URIEncoding", "UTF-8");
        tomcat.getConnector().setProperty("connectionTimeout", "60000");

        tomcat.getConnector().setProperty("maxKeepAliveRequests", "-1");
        tomcat.getConnector().setProtocol("org.apache.coyote.http11.Http11NioProtocol");

        Context context = tomcat.addContext("/", baseDir);
        Tomcat.addServlet(context, "dispatcher", new DispatcherServlet());
        context.addServletMapping("/*", "dispatcher");
        ServletManager.getInstance().addServletContext(address.getPort(), context.getServletContext());

        try {
            tomcat.start();
        } catch (LifecycleException e) {
            throw new IllegalStateException("Failed to start tomcat server at " + address, e);
        }
    }

    @Override
    public void close() throws IOException {
        ServletManager.getInstance().removeServletContext(address.getPort());

        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (Exception e) {
            logger.error("stop tomcat err {}", e);
        }
    }
}
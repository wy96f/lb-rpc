package cn.v5.lbrpc.http.server;

import cn.v5.lbrpc.common.client.core.exceptions.RpcException;
import cn.v5.lbrpc.common.client.core.loadbalancer.ServiceRegistration;
import cn.v5.lbrpc.common.server.AbstractServerFactory;
import com.google.common.base.Preconditions;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by yangwei on 15-6-6.
 */
public class ContainerServer extends AbstractDeployServer {
    private static final Logger logger = LoggerFactory.getLogger(ContainerServer.class);

    private final HttpServletDispatcher dispatcher = new HttpServletDispatcher();
    private final ResteasyDeployment deployment = new ResteasyDeployment();
    private final String proto;
    private AbstractHttpServer server;

    protected ContainerServer(InetSocketAddress address, List<Object> interceptors, ServiceRegistration registration, String proto) {
        super(address, interceptors, registration);
        this.proto = proto;
    }

    @Override
    protected void doStart() {
        bindServer();

        ServletContext servletContext = ServletManager.getInstance().getServletContext(address.getPort());
        if (servletContext == null) {
            servletContext = ServletManager.getInstance().getServletContext(ServletManager.EXTERNAL_SERVER_PORT);
        }
        if (servletContext == null) {
            throw new RpcException("No servlet context found. If you are using server='servlet', " +
                    "make sure that you've configured " + BootstrapListener.class.getName() + " in web.xml");
        }

        servletContext.setAttribute(ResteasyDeployment.class.getName(), deployment);

        try {
            dispatcher.init(new SimpleServletConfig(servletContext));
        } catch (ServletException e) {
            throw new RpcException(e);
        }
    }

    @Override
    protected ResteasyDeployment getDeployment() {
        return deployment;
    }

    @Override
    public void stop() {
        try {
            logger.info("starting stopping http server on {}", address);
            server.close();
            logger.info("Stop listening on {}", address);
        } catch (IOException e) {
            logger.error("container server close err {}", e);
        }
    }

    void bindServer() {
        if (proto.compareTo(AbstractServerFactory.TOMCAT_CONTAINER) == 0) {
            server = new TomcatHttpServer(address, new RestHandler());
        } else {
            Preconditions.checkArgument(false, proto + " not supported for binding http server");
        }
    }

    @Override
    public String getProto() {
        return proto;
    }

    private static class SimpleServletConfig implements ServletConfig {

        private final ServletContext servletContext;

        public SimpleServletConfig(ServletContext servletContext) {
            this.servletContext = servletContext;
        }

        public String getServletName() {
            return "DispatcherServlet";
        }

        public ServletContext getServletContext() {
            return servletContext;
        }

        public String getInitParameter(String s) {
            return null;
        }

        public Enumeration getInitParameterNames() {
            return new Enumeration() {
                public boolean hasMoreElements() {
                    return false;
                }

                public Object nextElement() {
                    return null;
                }
            };
        }
    }

    private class RestHandler implements HttpHandler {
        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            dispatcher.service(request, response);
        }
    }
}

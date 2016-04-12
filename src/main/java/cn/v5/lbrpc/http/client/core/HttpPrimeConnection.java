package cn.v5.lbrpc.http.client.core;

import cn.v5.lbrpc.common.client.core.CloseFuture;
import cn.v5.lbrpc.common.client.core.Host;
import cn.v5.lbrpc.common.client.core.IPrimeConnection;
import cn.v5.lbrpc.common.client.core.exceptions.ConnectionException;
import cn.v5.lbrpc.common.client.core.exceptions.TransportException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangwei on 15-6-9.
 */
public class HttpPrimeConnection implements IPrimeConnection {
    private static final Logger logger = LoggerFactory.getLogger(HttpPrimeConnection.class);
    private final Host host;
    private HttpClient client;

    public HttpPrimeConnection(Host host) {
        this.host = host;
        client = new DefaultHttpClient();
        HttpConnectionParams.setConnectionTimeout(client.getParams(), 2000);
    }

    public IPrimeConnection connect() throws ConnectionException {
        String url = "http://" + host.getAddress().getHostString() + ":" + host.getAddress().getPort();
        HttpUriRequest get = new HttpGet(url);
        HttpResponse response = null;
        try {
            response = client.execute(get);
            if (logger.isDebugEnabled() && response.getStatusLine() != null) {
                logger.debug("Response code:" + response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            throw new TransportException(host.getAddress(), e, "Cannot connect");
        } finally {
            get.abort();
        }
        return this;
    }

    @Override
    public CloseFuture closeAsync() {
        return null;
    }
}

package cn.v5.lbrpc.http;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by yangwei on 15-5-11.
 */
@Path("echoService")
@Consumes(value = MediaType.APPLICATION_JSON)
@Produces(value=MediaType.APPLICATION_JSON)
public interface RestEchoService {
    @POST
    @Path("echo_post")
    public RestEchoInfo echoPost(RestEchoInfo info);

    @PUT
    @Path("echo_put")
    public RestEchoInfo echoPut(RestEchoInfo info);

    @GET
    @Path("echo_get")
    public String echoGet(@QueryParam("x") String x);

    @POST
    @Path("echo_post_io_exception")
    public RestEchoInfo echoPostIOException(RestEchoInfo info) throws IOException;

    @GET
    @Path("echo_get_runtime_exception")
    public String echoGetRuntimeException(@QueryParam("x") String x);

    @PUT
    @Path("echo_put_timeout")
    public RestEchoInfo echoPutTimeout(RestEchoInfo info);

    @GET
    @Path("echo_return_null")
    public RestEchoInfo echoReturnNull();

    @GET
    @Path("echo_return_void")
    public void echoReturnVoid();

    @POST
    @Path("echo_with_null_parameter")
    public String echoWithNullParameter(RestEchoInfo info);
}
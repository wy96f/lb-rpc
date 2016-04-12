package cn.v5.lbrpc.http;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by yangwei on 27/8/15.
 */
@Path("echoService")
@Consumes(value = MediaType.APPLICATION_JSON)
@Produces(value=MediaType.APPLICATION_JSON)
public interface HttpNonExistentService {
    @GET
    @Path("request_non_existent_service")
    public void requestNonExistentService();
}

package edu.utexas.tacc.tapis.files.api.routes;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@Path("/systems")
public class Systems {

    @GET
    @Path("/}")
    @Produces("application/json")
    public Response getSystems() throws WebApplicationException {

        return Response.ok("ok").build();
    }

}

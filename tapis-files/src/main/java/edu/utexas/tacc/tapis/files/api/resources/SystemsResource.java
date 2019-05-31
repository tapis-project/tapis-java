package edu.utexas.tacc.tapis.files.api.resources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("systems")
public class SystemsResource {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSystems() throws WebApplicationException {

        return Response.ok("ok").build();
    }

    @GET
    @Path("{systemId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSystemByID(@PathParam("systemId") int systemId) throws WebApplicationException {

        return Response.ok(String.format("Looking at system: %s", systemId)).build();
    }

}

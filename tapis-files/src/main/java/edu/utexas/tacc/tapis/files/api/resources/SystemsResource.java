package edu.utexas.tacc.tapis.files.api.resources;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import edu.utexas.tacc.tapis.files.lib.dao.StorageSystemsDAO;
import edu.utexas.tacc.tapis.files.lib.models.StorageSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.models.AuthenticatedUser;
import io.swagger.v3.oas.annotations.responses.ApiResponse;


@Path("systems")
@Produces(MediaType.APPLICATION_JSON)
public class SystemsResource {

    @Inject
    private StorageSystemsDAO systemsDAO;

    @GET
    @ApiResponse(
            description = "List of systems."
    )
    public List<StorageSystem> getSystems(@Context SecurityContext sc) throws WebApplicationException, TapisException, SQLException {
        AuthenticatedUser user = (AuthenticatedUser) sc.getUserPrincipal();
        List<StorageSystem> results = systemsDAO.listSystems(user.getUsername(), user.getTenantId());
        return results;
    }

    @GET
    @Path("{systemId}")
    public Map getSystemByID(@Context SecurityContext sc, @PathParam("systemId") long systemId) throws WebApplicationException {
        Map<String, String> data = new HashMap<>();
        data.put("1", "abc");
        data.put("2", "def");
        data.put("3", "ghi");
        return data;

    }

}

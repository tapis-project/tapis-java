package edu.utexas.tacc.tapis.systems.api.resources;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqCreateGrant;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@Path("/grant")
public class GrantResource
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(GrantResource.class);

  // Json schema resource files.
  private static final String FILE_GRANT_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/GrantCreateRequest.json";

  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  /* Jax-RS context dependency injection allows implementations of these abstract
   * types to be injected (ch 9, jax-rs 2.0):
   *
   *      javax.ws.rs.container.ResourceContext
   *      javax.ws.rs.core.Application
   *      javax.ws.rs.core.HttpHeaders
   *      javax.ws.rs.core.Request
   *      javax.ws.rs.core.SecurityContext
   *      javax.ws.rs.core.UriInfo
   *      javax.ws.rs.core.Configuration
   *      javax.ws.rs.ext.Providers
   *
   * In a servlet environment, Jersey context dependency injection can also
   * initialize these concrete types (ch 3.6, jersey spec):
   *
   *      javax.servlet.HttpServletRequest
   *      javax.servlet.HttpServletResponse
   *      javax.servlet.ServletConfig
   *      javax.servlet.ServletContext
   *
   * Inject takes place after constructor invocation, so fields initialized in this
   * way can not be accessed in constructors.
   */
  @Context
  private HttpHeaders _httpHeaders;
  @Context
  private Application _application;
  @Context
  private UriInfo _uriInfo;
  @Context
  private SecurityContext _securityContext;
  @Context
  private ServletContext _servletContext;
  @Context
  private HttpServletRequest _request;

  // **************** Inject Services ****************
//  @com.google.inject.Inject
  private SystemsService systemsService;

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Create a user grant for a system
   * @param prettyPrint - pretty print the output
   * @param payloadStream - request body
   * @return basic response
   */
  @POST
  @Path("/grant/{systemName}/user/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Create a grant in the Security Kernel giving a user permissions for a system",
    description =
        "Create a grant in the Security Kernel for a user using a request body. Requester must be owner of " +
        "the system. Permissions that can be requested: read, write, execute or '*' to indicate all permisions.",
    tags = "grant",
    requestBody =
      @RequestBody(
        description = "A JSON object specifying information for the grant to be created.",
        required = true,
        content = @Content(schema = @Schema(implementation = ReqCreateGrant.class))
      ),
    responses = {
      @ApiResponse(responseCode = "200", description = "Grant created.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response createGrant(@PathParam("systemName") String systemName,
                              @PathParam("userName") String userName,
                              @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint,
                              InputStream payloadStream)
  {
    String msg;
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "createGrant",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // Check that we have all we need from the context, tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get tenant and apiUserId from context
    String tenantName = threadContext.getTenantId();
    String apiUserId = threadContext.getUser();

    // ------------------------- Validate Payload -------------------------
    // Read the payload into a string.
    String json;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("SYSAPI_GRANT_JSON_ERROR", systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_GRANT_CREATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("SYSAPI_GRANT_JSON_INVALID", systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    JsonObject obj = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);

    systemsService = new SystemsServiceImpl();
    // Check that the system exists
    boolean systemExists;
    try { systemExists = systemsService.checkForSystemByName(tenantName, systemName); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_GRANT_CREATE_ERROR", null, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (!systemExists)
    {
      msg = MsgUtils.getMsg("SYSAPI_GRANT_NOSYSTEM", systemName, userName);
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Get the system owner and verify that requester is the owner
    String owner;
    try { owner = systemsService.getSystemOwner(tenantName, systemName); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("SYSAPI_GET_OWNER_ERROR", systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (StringUtils.isBlank(owner))
    {
      msg = MsgUtils.getMsg("SYSAPI_GET_OWNER_EMPTY", systemName);
      _log.error(msg);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (!owner.equals(apiUserId))
    {
      msg = MsgUtils.getMsg("SYSAPI_NOT_OWNER", systemName, "createGrant");
      _log.error(msg);
      return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Extract requested permissions from the request body
    // Json schema validation should ensure we have at least 1 item
    StringBuilder permsListSB = new StringBuilder();
    JsonArray perms = null;
    if (obj.has("permissions")) perms = obj.getAsJsonArray("permissions");
    if (perms != null && perms.size() > 0)
    {
      for (int i = 0; i < perms.size()-1; i++)
      {
        permsListSB.append(perms.get(i).toString()).append(",");
      }
      permsListSB.append(perms.get(perms.size()-1).toString());
    }

    // TODO It would be good to collect and report as many errors as possible so they can all be fixed before next attempt
    msg = null;
    // Check values. We should have at least one permission
    if (perms == null || perms.size() <= 0)
    {
      msg = MsgUtils.getMsg("SYSAPI_GRANT_NOPERMS", systemName, userName);
    }

    // If validation failed log error message and return response
    if (msg != null)
    {
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Make the service call to create the grant
    try
    {
      systemsService.createUserGrant(tenantName, systemName, userName, permsListSB.toString());
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_GRANT_CREATE_ERROR", null, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    RespBasic resp1 = new RespBasic();
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(ApiUtils.getMsg("SYSAPI_GRANT_CREATED", null, systemName,
                                                                   userName, permsListSB.toString()),
                                                   prettyPrint, resp1))
      .build();
  }

  /**
   * getSystemByName
   * @param name - name of the system
   * @param prettyPrint - pretty print the output
   * @param getCreds - should credentials be included
   * @return Response with system object as the result
   */
  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Retrieve information for a system",
      description =
          "Retrieve information for a system given the system name. " +
          "Use query parameter returnCredentials = true to have the user access credentials " +
          "included in the response.",
      tags = "systems",
// TODO
//      parameters = {
//          @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
//              description = "Pretty print the response"),
//          @Parameter(in = ParameterIn.QUERY, name = "returnCredentials", required = false,
//              description = "Include the credentials in the response")
//      },
      responses = {
          @ApiResponse(responseCode = "200", description = "System found.",
            content = @Content(schema = @Schema(implementation = RespSystem.class))),
          @ApiResponse(responseCode = "400", description = "Input error.",
            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
          @ApiResponse(responseCode = "404", description = "System not found.",
            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
          @ApiResponse(responseCode = "401", description = "Not authorized.",
            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
          @ApiResponse(responseCode = "500", description = "Server error.",
            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
      }
  )
  public Response getSystemByName(@PathParam("name") String name,
                                  @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint,
                                  @QueryParam("returnCredentials") @DefaultValue("false") boolean getCreds)
  {
    systemsService = new SystemsServiceImpl();
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSystemByName",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get tenant and apiUserId from context
    String tenant = threadContext.getTenantId();
    String apiUserId = threadContext.getUser();

    TSystem system;
    try
    {
      system = systemsService.getSystemByName(tenant, name, apiUserId, getCreds);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_GET_NAME_ERROR", null, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // The specified job was not found for the tenant.
    if (system == null)
    {
      String msg = ApiUtils.getMsg("SYSAPI_NOT_FOUND", null, name);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(MsgUtils.getMsg("TAPIS_NOT_FOUND", "System", name),
                                               prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSystem resp1 = new RespSystem(system);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
        MsgUtils.getMsg("TAPIS_FOUND", "System", name), prettyPrint, resp1)).build();
  }

  /**
   * getSystemNames
   * @param prettyPrint - pretty print the output
   * @return - list of system names
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Retrieve list of system names",
    description = "Retrieve list of system names.",
    tags = "systems",
// TODO
//    parameters = {
//      @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
//        description = "Pretty print the response")
//    },
    responses = {
      @ApiResponse(responseCode = "200", description = "Success.",
                   content = @Content(schema = @Schema(implementation = RespNameArray.class))
      ),
      @ApiResponse(responseCode = "400", description = "Input error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response getSystemNames(@QueryParam("pretty") @DefaultValue("false") boolean prettyPrint)
  {
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSystems",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get tenant and apiUserId from context
    String tenant = threadContext.getTenantId();
    String apiUserId = threadContext.getUser();

    // ------------------------- Retrieve all records -----------------------------
    systemsService = new SystemsServiceImpl();
    List<String> systemNames;
    try { systemNames = systemsService.getSystemNames(tenant); }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_SELECT_ERROR", null, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    if (systemNames == null) systemNames = Collections.emptyList();
    int cnt = systemNames.size();
    ResultNameArray names = new ResultNameArray();
    names.names = systemNames.toArray(new String[0]);
    RespNameArray resp1 = new RespNameArray(names);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
        MsgUtils.getMsg("TAPIS_FOUND", "Systems", cnt + " items"), prettyPrint, resp1)).build();
  }

  /**
   * deleteSystemByName
   * @param name - name of the system to delete
   * @param prettyPrint - pretty print the output
   * @return - response with change count as the result
   */
  @DELETE
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Delete a system given the system name",
    description = "Delete a system given the system name. ",
    tags = "systems",
// TODO
//      parameters = {
//          @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
//              description = "Pretty print the response")
//      },
    responses = {
      @ApiResponse(responseCode = "200", description = "System deleted.",
        content = @Content(schema = @Schema(implementation = RespChangeCount.class))),
      @ApiResponse(responseCode = "400", description = "Input error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response deleteSystemByName(@PathParam("name") String name,
                                  @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint)
  {
    systemsService = new SystemsServiceImpl();
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "deleteSystemByName",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get tenant and apiUserId from context
    String tenant = threadContext.getTenantId();
    String apiUserId = threadContext.getUser();

    int changeCount;
    try
    {
      changeCount = systemsService.deleteSystemByName(tenant, name);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_DELETE_NAME_ERROR", null, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we deleted the system.
    // Return the number of objects impacted.
    ResultChangeCount count = new ResultChangeCount();
    count.changes = changeCount;
    RespChangeCount resp1 = new RespChangeCount(count);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
      MsgUtils.getMsg("TAPIS_DELETED", "System", name), prettyPrint, resp1)).build();
  }
}

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
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPerms;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * JAX-RS REST resource for Tapis System permissions
 * Contains annotations which generate the OpenAPI specification documents.
 * Annotations map HTTP verb + endpoint to method invocation.
 * Permissions are stored in the Security Kernel
 *
 */
@Path("/perms")
public class PermsResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(PermsResource.class);

  // Json schema resource files.
  private static final String FILE_PERMS_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/PermsRequest.json";

  // Field names used in Json
  private static final String PERMISSIONS_FIELD = "permissions";

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
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

  // **************** Inject Services using HK2 ****************
  @Inject
  private SystemsService systemsService;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Assign specified permissions for given system and user.
   * @param prettyPrint - pretty print the output
   * @param payloadStream - request body
   * @return basic response
   */
  @POST
  @Path("/{systemName}/user/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Create permissions in the Security Kernel giving a user access to a system",
    description =
        "Create permissions in the Security Kernel for a user using a request body. Requester must be owner of " +
        "the system. Permissions: READ, MODIFY, DELETE or '*' to indicate all permissions.",
    tags = "permissions",
    requestBody =
      @RequestBody(
        description = "A JSON object specifying a list of permissions.",
        required = true,
        content = @Content(schema = @Schema(implementation = ReqPerms.class))
      ),
    responses = {
      @ApiResponse(responseCode = "200", description = "Permissions granted.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response grantUserPerms(@PathParam("systemName") String systemName,
                                 @PathParam("userName") String userName,
                                 @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint,
                                 InputStream payloadStream)
  {
    String msg;
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "grantUserPerms",
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

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(systemsService, tenantName, systemName, userName, prettyPrint, "grantUserPerms");
    if (resp != null) return resp;
    // ------------------------- Check authorization -------------------------
    resp = ApiUtils.checkAuth1(systemsService, tenantName, systemName, userName, prettyPrint, apiUserId,
                       "grantUserPerms", true);
    if (resp != null) return resp;

    // ------------------------- Extract and validate payload -------------------------
    var permsList = new ArrayList<String>();
    resp = checkAndExtractPayload(systemName, userName, prettyPrint, payloadStream, permsList);
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to assign the permissions
    try
    {
      systemsService.grantUserPermissions(tenantName, systemName, userName, permsList);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_ERROR", null, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    RespBasic resp1 = new RespBasic();
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(ApiUtils.getMsg("SYSAPI_PERMS_GRANTED", null, systemName,
                                                                   userName, String.join(",", permsList)),
                                                   prettyPrint, resp1))
      .build();
  }

  /**
   * getUserPerms
   * @param prettyPrint - pretty print the output
   * @return Response with list of permissions
   */
  @GET
  @Path("/{systemName}/user/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Retrieve system related permissions for given system and user",
      description =
          "Retrieve all system related permissions for a given system and user.",
      tags = "permissions",
      responses = {
          @ApiResponse(responseCode = "200", description = "Success.",
            content = @Content(schema = @Schema(implementation = RespNameArray.class))),
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
  public Response getUserPerms(@PathParam("systemName") String systemName,
                                @PathParam("userName") String userName,
                                @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint)
  {
    String msg;
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getUserPerms",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get tenant and apiUserId from context
    String tenantName = threadContext.getTenantId();
//    String apiUserId = threadContext.getUser();

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(systemsService, tenantName, systemName, userName, prettyPrint, "getUserPerms");
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to get the permissions
    List<String> permNames;
    try { permNames = systemsService.getUserPermissions(tenantName, systemName, userName); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_ERROR", null, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    if (permNames == null) permNames = Collections.emptyList();
    int cnt = permNames.size();
    ResultNameArray names = new ResultNameArray();
    names.names = permNames.toArray(new String[0]);
    RespNameArray resp1 = new RespNameArray(names);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
      MsgUtils.getMsg("TAPIS_FOUND", "System permissions", cnt + " items"), prettyPrint, resp1)).build();
  }

  /**
   * Revoke permission for given system and user.
   * @param prettyPrint - pretty print the output
   * @return basic response
   */
  @DELETE
  @Path("/{systemName}/user/{userName}/{permission}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Revoke specified permission in the Security Kernel",
    description =
      "Revoke permission in the Security Kernel for a user. Requester must be owner of " +
        "the system. Permissions: READ, MODIFY or DELETE.",
    tags = "permissions",
    responses = {
      @ApiResponse(responseCode = "200", description = "Permission revoked.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response revokeUserPerm(@PathParam("systemName") String systemName,
                                 @PathParam("userName") String userName,
                                 @PathParam("permission") String permission,
                                 @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint)
  {
    String msg;
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "revokeUserPerm",
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

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(systemsService, tenantName, systemName, userName, prettyPrint, "revokeUserPerm");
    if (resp != null) return resp;
    // ------------------------- Check authorization -------------------------
    resp = ApiUtils.checkAuth1(systemsService, tenantName, systemName, userName, prettyPrint, apiUserId,
                       "revokeUserPerm", false);
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to revoke the permissions
    var permsList = new ArrayList<String>();
    permsList.add(permission);
    try
    {
      systemsService.revokeUserPermissions(tenantName, systemName, userName, permsList);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_ERROR", null, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(ApiUtils.getMsg("SYSAPI_PERMS_REVOKED", null, systemName,
                                                                   userName, String.join(",", permsList)),
                                                   prettyPrint, resp1))
      .build();
  }

  /**
   * Revoke permissions for given system and user.
   * @param prettyPrint - pretty print the output
   * @param payloadStream - request body
   * @return basic response
   */
  @POST
  @Path("/{systemName}/user/{userName}/revoke")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Revoke system permissions in the Security Kernel using request body",
    description =
      "Revoke permissions in the Security Kernel for a user using a request body. Requester must be owner of " +
        "the system. Permissions: READ, MODIFY, DELETE or '*' to indicate all permissions.",
    tags = "permissions",
    requestBody =
    @RequestBody(
      description = "A JSON object specifying a list of permissions.",
      required = true,
      content = @Content(schema = @Schema(implementation = ReqPerms.class))
    ),
    responses = {
      @ApiResponse(responseCode = "200", description = "Permission revoked.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response revokeUserPerms(@PathParam("systemName") String systemName,
                                 @PathParam("userName") String userName,
                                 @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint,
                                 InputStream payloadStream)
  {
    String msg;
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "revokeUserPerms",
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

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(systemsService, tenantName, systemName, userName, prettyPrint, "revokeUserPerms");
    if (resp != null) return resp;
    // ------------------------- Check authorization -------------------------
    resp = ApiUtils.checkAuth1(systemsService, tenantName, systemName, userName, prettyPrint, apiUserId,
                       "revokeUserPerms", false);
    if (resp != null) return resp;

    // ------------------------- Extract and validate payload -------------------------
    var permsList = new ArrayList<String>();
    resp = checkAndExtractPayload(systemName, userName, prettyPrint, payloadStream, permsList);
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to revoke the permissions
    try
    {
      systemsService.revokeUserPermissions(tenantName, systemName, userName, permsList);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_ERROR", null, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(ApiUtils.getMsg("SYSAPI_PERMS_REVOKED", null, systemName,
                                                                   userName, String.join(",", permsList)),
                                                   prettyPrint, resp1))
      .build();
  }


  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Check json payload and extract permissions list.
   * @param systemName - name of the system, for constructing response msg
   * @param userName - name of user associated with the perms request, for constructing response msg
   * @param prettyPrint - print flag used to construct response
   * @param payloadStream - Stream for extracting request json
   * @param permsList - List for resulting permissions extracted from payload
   * @return - null if all checks OK else Response containing info
   */
  private Response checkAndExtractPayload(String systemName, String userName, boolean prettyPrint,
                                          InputStream payloadStream, List<String> permsList)
  {
    Response resp = null;
    String msg;
    // Read the payload into a string.
    String json;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_JSON_ERROR", systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_PERMS_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_JSON_INVALID", systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    JsonObject obj = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);

    // Extract permissions from the request body
    JsonArray perms = null;
    if (obj.has(PERMISSIONS_FIELD)) perms = obj.getAsJsonArray(PERMISSIONS_FIELD);
    if (perms != null && perms.size() > 0)
    {
      for (int i = 0; i < perms.size(); i++) { permsList.add(StringUtils.remove(perms.get(i).toString(),'"')); }
    }

    // TODO It would be good to collect and report as many errors as possible so they can all be fixed before next attempt
    msg = null;
    // Check values. We should have at least one permission
    if (perms == null || perms.size() <= 0)
    {
      msg = ApiUtils.getMsg("SYSAPI_PERMS_NOPERMS", systemName, userName);
    }

    // If validation failed log error message and return response
    if (msg != null)
    {
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return resp;
  }
}

package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqCreateSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
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

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

@Path("/system")
public class SystemResource
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemResource.class);

  // Json schema resource files.
  private static final String FILE_SYSTEMS_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemCreateRequest.json";

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
   * Create a system
   * @param prettyPrint - pretty print the output
   * @param payloadStream - request body
   * @return response containing reference to created object
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Create a system",
    description =
        "Create a system using a request body. " +
        "System name must be unique within a tenant and can be composed of alphanumeric characters " +
        "and the following special characters: [-._~]. Name must begin with an alphabetic character " +
        "and can be no more than 300 characters in length. " +
        "Description is optional with a maximum length of 2048 characters.",
    tags = "systems",
// TODO
//    parameters = {
//      @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
//                 description = "Pretty print the response")
//    },
    requestBody =
      @RequestBody(
        description = "A JSON object specifying information for the system to be created.",
        required = true,
        content = @Content(schema = @Schema(implementation = ReqCreateSystem.class))
      ),
    responses = {
      @ApiResponse(responseCode = "201", description = "System created.",
                   content = @Content(schema = @Schema(implementation = RespResourceUrl.class))
      ),
      @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "401", description = "Not authorized.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
      @ApiResponse(responseCode = "409", description = "System already exists.",
                   content = @Content(schema = @Schema(implementation = RespResourceUrl.class))),
      @ApiResponse(responseCode = "500", description = "Server error.",
        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
    }
  )
  public Response createSystem(@QueryParam("pretty") @DefaultValue("false") boolean prettyPrint, InputStream payloadStream)
  {
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "createSystem",
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



    // ------------------------- Validate Payload -------------------------
    // Read the payload into a string.
    String json;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "post system", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create validator specification.
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SYSTEMS_CREATE_REQUEST);

    // Make sure the json conforms to the expected schema.
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    String name, description, owner, host, bucketName, rootDir,
           jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId, tags, notes;
    String accessMech, proxyHost;
    // TODO: creds might need to be byte array
    String accessCred;
    int port, proxyPort;
    boolean available, useProxy;

    JsonObject obj = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);
    // Extract required name value and check to see if object exists.
    name = obj.get("name").getAsString();
    // TODO: Check if object exists. If yes then return 409 - Conflict

    // Extract other top level properties: description, owner, host ...
    // Extract required values
    host = obj.get("host").getAsString();
    // Extract optional values
    description = ApiUtils.getValS(obj.get("description"), "");
    owner = ApiUtils.getValS(obj.get("owner"), "");
    bucketName = ApiUtils.getValS(obj.get("bucketName"), "");
    rootDir = ApiUtils.getValS(obj.get("rootDir"), "");
    jobInputDir = ApiUtils.getValS(obj.get("jobInputDir"), "");
    jobOutputDir = (obj.has("jobOutputDir") ? obj.get("jobOutputDir").getAsString() : "");
    workDir = (obj.has("workDir") ? obj.get("workDir").getAsString() : "");
    scratchDir = (obj.has("scratchDir") ? obj.get("scratchDir").getAsString() : "");
    effectiveUserId = (obj.has("effectiveUserId") ? obj.get("effectiveUserId").getAsString(): "");
    available = (obj.has("available") ? obj.get("available").getAsBoolean() : true);
    accessCred = (obj.has("accessCredential") ? obj.get("accessCredential").getAsString() : "");
    tags = ApiUtils.getValS(obj.get("tags"), "{}");
    notes = ApiUtils.getValS(obj.get("notes"), "{}");

    //Extract Protocol information
    accessMech = (obj.has("accessMechanism") ? obj.get("accessMechanism").getAsString() : "NONE");
    port = (obj.has("port") ? obj.get("port").getAsInt() : -1);
    useProxy = (obj.has("useProxy") ? obj.get("useProxy").getAsBoolean() : false);
    proxyHost = (obj.has("proxyHost") ? obj.get("proxyHost").getAsString() : "");
    proxyPort = (obj.has("proxyPort") ? obj.get("proxyPort").getAsInt() : -1);
    // Extract list of supported transfer mechanisms contained in protocol
    // If element is not there or the list is empty then build empty array "{}"
    StringBuilder transferMechs = new StringBuilder("{");
    JsonArray mechs = null;
    if (obj.has("transferMechanisms")) mechs = obj.getAsJsonArray("transferMechanisms");
    if (mechs != null && mechs.size() > 0)
    {
      for (int i = 0; i < mechs.size()-1; i++)
      {
        transferMechs.append(mechs.get(i).toString()).append(",");
      }
      transferMechs.append(mechs.get(mechs.size()-1).toString());
    }
    transferMechs.append("}");

    // Check values.
    String msg = null;
    if (StringUtils.isBlank(name)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty name.");
    }
    else if (StringUtils.isBlank(host)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty host.");
    }
    else if (StringUtils.isBlank(accessMech)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty AccessProtocol mechanism.");
    }
    // If validation failed log error message and return response
    if (msg != null)
    {
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ------------------------- Create System Object ---------------------
    systemsService = new SystemsServiceImpl();
    try
    {
      systemsService.createSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                                  jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId, tags, notes,
                                  accessCred, accessMech, transferMechs.toString(), port, useProxy, proxyHost, proxyPort);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_ERROR", null, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + name;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
      ApiUtils.getMsg("SYSAPI_CREATED", null, name), prettyPrint, resp1)).build();
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
      system = systemsService.getSystemByName(tenant, name, getCreds);
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

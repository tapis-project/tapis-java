package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
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

import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;

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

  // TODO Remove hard coded values
  private static final String tenant = "tenant1";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Create a system
   * @param prettyPrint
   * @param payloadStream
   * @return
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
        "and can be no more than 256 characters in length. " +
        "Description is optional with a maximum length of 2048 characters.",
    tags = "systems",
    parameters = {
      @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
                 description = "Pretty print the response")
    },
    requestBody =
      @RequestBody(
      description = "A JSON object specifying information for the system to be created.",
      required = true,
      content = @Content(mediaType = MediaType.APPLICATION_JSON,
                         schema = @Schema(implementation = edu.utexas.tacc.tapis.systems.api.requestBody.ReqCreateSystem.class)
                        )
      ),
    responses = {
      @ApiResponse(responseCode = "201", description = "System created."),
      @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON."),
      @ApiResponse(responseCode = "401", description = "Not authorized."),
      @ApiResponse(responseCode = "409", description = "System already exists."),
      @ApiResponse(responseCode = "500", description = "Server error.")
    }
  )
  public Response createSystem(@QueryParam("pretty") @DefaultValue("false") boolean prettyPrint,
                               InputStream payloadStream)
  {
    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "createSystem",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // ------------------------- Validate Payload -------------------------
    // Read the payload into a string.
    String json = null;
    try { json = IOUtils.toString(payloadStream, Charset.forName("UTF-8")); }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "post system", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create validator specification.
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SYSTEMS_CREATE_REQUEST);

    // Make sure the json conforms to the expected schema.
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    String name, description, owner, host, bucketName, rootDir,
           jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId;
    String cmdMech, cmdProxyHost, txfMech, txfProxyHost;
    // TODO: creds might need to be byte array
    String cmdCred, txfCred;
    int cmdPort, cmdProxyPort, txfPort, txfProxyPort;
    boolean available, cmdUseProxy, txfUseProxy;

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
    cmdCred = (obj.has("commandCredential") ? obj.get("commandCredential").getAsString() : "");
    txfCred = (obj.has("transferCredential") ? obj.get("transferCredential").getAsString() : "");

    //Extract CommandProtocol and TransferProtocol properties
    JsonObject cmdProtObj = obj.getAsJsonObject("commandProtocol");
    cmdMech = (cmdProtObj.has("mechanism") ? cmdProtObj.get("mechanism").getAsString() : "NONE");
    cmdPort = (cmdProtObj.has("port") ? cmdProtObj.get("port").getAsInt() : -1);
    cmdUseProxy = (cmdProtObj.has("useProxy") ? cmdProtObj.get("useProxy").getAsBoolean() : false);
    cmdProxyHost = (cmdProtObj.has("proxyHost") ? cmdProtObj.get("proxyHost").getAsString() : "");
    cmdProxyPort = (cmdProtObj.has("proxyPort") ? cmdProtObj.get("proxyPort").getAsInt() : -1);
    JsonObject txfProtObj = obj.getAsJsonObject("transferProtocol");
    txfMech = (txfProtObj.has("mechanism") ? txfProtObj.get("mechanism").getAsString() : "NONE");
    txfPort = (txfProtObj.has("port") ? txfProtObj.get("port").getAsInt() : -1);
    txfUseProxy = (txfProtObj.has("useProxy") ? txfProtObj.get("useProxy").getAsBoolean() : false);
    txfProxyHost = (txfProtObj.has("proxyHost") ? txfProtObj.get("proxyHost").getAsString() : "");
    txfProxyPort = (txfProtObj.has("proxyPort") ? txfProtObj.get("proxyPort").getAsInt() : -1);

    // Check values.
    String msg = null;
    if (StringUtils.isBlank(name)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty name.");
    }
    else if (StringUtils.isBlank(host)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty host.");
    }
    else if (StringUtils.isBlank(cmdMech)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty CommandProtocol mechanism.");
    }
    else if (StringUtils.isBlank(txfMech)) {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty TransferProtocol mechanism.");
    }
    // If validation failed log error message and return response
    if (msg != null)
    {
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ------------------------- Create System Object ---------------------
    try
    {
      // TODO Use static factory method, or better yet use DI, maybe Guice
      SystemsService svc = new SystemsService();
      svc.createSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                       jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId,
                       cmdMech, cmdPort, cmdUseProxy, cmdProxyHost, cmdProxyPort,
                       txfMech, txfPort, txfUseProxy, txfProxyHost, txfProxyPort,
                       cmdCred, txfCred);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_ERROR", null, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    return Response.status(Status.CREATED).entity(RestUtils.createSuccessResponse(
      ApiUtils.getMsg("SYSAPI_CREATED", null, name), prettyPrint, "Created system object")).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* getSystemByName:                                                             */
  /* ---------------------------------------------------------------------------- */
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
      parameters = {
          @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
              description = "Pretty print the response"),
          @Parameter(in = ParameterIn.QUERY, name = "returnCredentials", required = false,
              description = "Include the credentials in the response")
      },
      responses = {
          @ApiResponse(responseCode = "200", description = "System found."),
          @ApiResponse(responseCode = "400", description = "Input error."),
          @ApiResponse(responseCode = "401", description = "Not authorized."),
          @ApiResponse(responseCode = "500", description = "Server error.")
      }
  )
  public Response getSystemByName(@PathParam("name") String name,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                  @DefaultValue("false") @QueryParam("returnCredentials") boolean getCreds)
  {
    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSystemByName",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // TODO Use static factory method, or better yet use DI, maybe Guice
    SystemsService svc = new SystemsService();
    TSystem system = null;
    try
    {
      system = svc.getSystemByName(tenant, name, getCreds);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_GET_NAME_ERROR", null, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // The specified job was not found for the tenant.
    if (system == null)
    {
      String msg = ApiUtils.getMsg("SYSAPI_NOT_FOUND", null, name);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(RestUtils.createErrorResponse(MsgUtils.getMsg("TAPIS_NOT_FOUND", "System", name),
                                               prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
        MsgUtils.getMsg("TAPIS_FOUND", "System", name), prettyPrint, system)).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* getSystems:                                                                  */
  /* ---------------------------------------------------------------------------- */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
    summary = "Retrieve all systems",
    description = "Retrieve all systems.",
    tags = "systems",
    parameters = {
      @Parameter(in = ParameterIn.QUERY, name = "pretty", required = false,
        description = "Pretty print the response")
    },
    responses = {
      @ApiResponse(responseCode = "200", description = "Success."),
      @ApiResponse(responseCode = "400", description = "Input error."),
      @ApiResponse(responseCode = "401", description = "Not authorized."),
      @ApiResponse(responseCode = "500", description = "Server error.")
    }
  )
  public Response getSystems(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
  {
    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSystems",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // ------------------------- Retrieve all records -----------------------------
    // TODO Use static factory method, or better yet use DI, maybe Guice
    SystemsService svc = new SystemsService();
    List<TSystem> systems = null;
    try { systems = svc.getSystems(tenant); }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_SELECT_ERROR", null, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    int cnt = (systems == null ? 0 : systems.size());
    return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
        MsgUtils.getMsg("TAPIS_FOUND", "Systems", cnt + " items"), prettyPrint, systems)).build();
  }
}

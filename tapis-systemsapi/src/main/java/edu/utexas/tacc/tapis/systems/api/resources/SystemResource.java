package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
  @Operation(
      summary = "Create a system using a request body",
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
      responses = {
          @ApiResponse(responseCode = "201", description = "System created."),
          @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON."),
          @ApiResponse(responseCode = "401", description = "Not authorized."),
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
    // Extract top level properties: name, description, owner, host ...
    name = obj.get("name").getAsString();
    description = obj.get("description").getAsString();
    owner = obj.get("owner").getAsString();
    host = obj.get("host").getAsString();
    bucketName = obj.get("bucketName").getAsString();
    rootDir = obj.get("rootDir").getAsString();
    jobInputDir = obj.get("jobInputDir").getAsString();
    jobOutputDir = obj.get("jobOutputDir").getAsString();
    workDir = obj.get("workDir").getAsString();
    scratchDir = obj.get("scratchDir").getAsString();
    effectiveUserId = obj.get("effectiveUserId").getAsString();
    available = obj.get("available").getAsBoolean();
    cmdCred = obj.get("commandCredential").getAsString();
    txfCred = obj.get("transferCredential").getAsString();
    //Extract CommandProtocol and TransferProtocol properties
    cmdMech = obj.get("commandProtocol.mechanism").getAsString();
    cmdPort = obj.get("commandProtocol.port").getAsInt();
    cmdUseProxy = obj.get("commandProtocol.useProxy").getAsBoolean();
    cmdProxyHost = obj.get("commandProtocol.proxyHost").getAsString();
    cmdProxyPort = obj.get("commandProtocol.proxyPort").getAsInt();
    txfMech = obj.get("transferProtocol.mechanism").getAsString();
    txfPort = obj.get("transferProtocol.port").getAsInt();
    txfUseProxy = obj.get("transferProtocol.useProxy").getAsBoolean();
    txfProxyHost = obj.get("transferProtocol.proxyHost").getAsString();
    txfProxyPort = obj.get("transferProtocol.proxyPort").getAsInt();

    // Check values.
    if (StringUtils.isBlank(name))
    {
      String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty name.");
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (StringUtils.isBlank(owner))
    {
      String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty owner.");
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (StringUtils.isBlank(host))
    {
      String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty host.");
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
      String msg = ApiUtils.getMsg("SYSAPI_CREATE_ERROR", null, name, e.getMessage());
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
      summary = "Retrieve information for a system given the system name.",
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
    int cnt = systems == null ? 0 : systems.size();
    return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
        MsgUtils.getMsg("TAPIS_FOUND", "Systems", cnt + " items"), prettyPrint, systems)).build();
  }
}

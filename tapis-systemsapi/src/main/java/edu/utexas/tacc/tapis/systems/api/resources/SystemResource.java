package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqCreateSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Capability.Category;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMethod;
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

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

/*
 * JAX-RS REST resource for a Tapis System (edu.utexas.tacc.tapis.systems.model.TSystem)
 * Contains annotations which generate the OpenAPI specification documents.
 * Annotations map HTTP verb + endpoint to method invocation.
 *
 */
@Path("/")
public class SystemResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemResource.class);

  // Json schema resource files.
  private static final String FILE_SYSTEM_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemCreateRequest.json";
  // String used to mask secrets in json
  private static final String SECRETS_MASK = "***";

  // Field names used in Json
  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String SYSTEM_TYPE_FIELD = "systemType";
  private static final String OWNER_FIELD = "owner";
  private static final String HOST_FIELD = "host";
  private static final String AVAILABLE_FIELD = "available";
  private static final String EFFECTIVE_USERID_FIELD = "effectiveUserId";
  private static final String ACCESS_METHOD_FIELD = "accessMethod";
  private static final String ACCESS_CREDENTIAL_FIELD = "accessCredential";
  private static final String BUCKET_NAME_FIELD = "bucketName";
  private static final String ROOT_DIR_FIELD = "rootDir";
  private static final String TRANSFER_METHODS_FIELD = "transferMethods";
  private static final String PORT_FIELD = "port";
  private static final String USE_PROXY_FIELD = "userProxy";
  private static final String PROXY_HOST_FIELD = "proxyHost";
  private static final String PROXY_PORT_FIELD = "proxyPort";
  private static final String JOB_CAN_EXEC_FIELD = "jobCanExec";
  private static final String JOB_LOCAL_WORKING_DIR_FIELD = "jobLocalWorkingDir";
  private static final String JOB_LOCAL_ARCHIVE_DIR_FIELD = "jobLocalArchiveDir";
  private static final String JOB_REMOTE_ARCHIVE_SYSTEM_FIELD = "jobRemoteArchiveSystem";
  private static final String JOB_REMOTE_ARCHIVE_DIR_FIELD = "jobRemoteArchiveDir";
  private static final String JOB_CAPABILITIES_FIELD = "jobCapabilities";
  private static final String JOB_CAPABILITY_CATEGORY_FIELD = "category";
  private static final String JOB_CAPABILITY_NAME_FIELD = "name";
  private static final String JOB_CAPABILITY_VALUE_FIELD = "value";
  private static final String TAGS_FIELD = "tags";
  private static final String NOTES_FIELD = "notes";


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

  // **************** Inject Services ****************
//  @com.google.inject.Inject
  private SystemsService systemsService;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

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
        "and can be no more than 256 characters in length. " +
        "Description is optional with a maximum length of 2048 characters.",
    tags = "systems",
// TODO Including parameter info here and in method sig results in duplicates in openapi spec.
// TODO    JAX-RS appears to require the annotations in the method sig
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
    String msg;
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context

    // Trace this request.
    if (_log.isTraceEnabled())
    {
      msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "createSystem",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get tenant and apiUserId from context
    String tenantName = threadContext.getTenantId();
    String apiUserId = threadContext.getUser();

    // ------------------------- Validate Payload -------------------------
    // Read the payload into a string.
    String rawJson;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "post system", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create validator specification and validate the json against the schema
    // TODO Json may contain secrets. Does validator do logging?
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_CREATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Get the Json object and prepare to extract info from it
    JsonObject jsonObject = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);

    String systemName, description, systemType, owner, host, effectiveUserId, accessMethodStr, bucketName, rootDir,
           proxyHost, tags, notes;
    String jobLocalWorkingDir, jobLocalArchiveDir, jobRemoteArchiveSystem, jobRemoteArchiveDir;
    int port, proxyPort;
    boolean available, useProxy, jobCanExec;

    // Extract top level properties: name, systemType, host, description, owner, ...
    // Extract required values
    systemName = jsonObject.get(NAME_FIELD).getAsString();
    systemType = jsonObject.get(SYSTEM_TYPE_FIELD).getAsString();
    host = jsonObject.get(HOST_FIELD).getAsString();
    accessMethodStr = jsonObject.get(ACCESS_METHOD_FIELD).getAsString();
    jobCanExec =  jsonObject.get(JOB_CAN_EXEC_FIELD).getAsBoolean();
    // Extract optional values
    description = ApiUtils.getValS(jsonObject.get(DESCRIPTION_FIELD), "");
    owner = ApiUtils.getValS(jsonObject.get(OWNER_FIELD), "");
    available = (jsonObject.has(AVAILABLE_FIELD) ? jsonObject.get(AVAILABLE_FIELD).getAsBoolean() : TSystem.DEFAULT_AVAILABLE);
    effectiveUserId = ApiUtils.getValS(jsonObject.get(EFFECTIVE_USERID_FIELD), "");
    bucketName = ApiUtils.getValS(jsonObject.get(BUCKET_NAME_FIELD), "");
    rootDir = ApiUtils.getValS(jsonObject.get(ROOT_DIR_FIELD), "");
    port = (jsonObject.has(PORT_FIELD) ? jsonObject.get(PORT_FIELD).getAsInt() : -1);
    useProxy = (jsonObject.has(USE_PROXY_FIELD) ? jsonObject.get(USE_PROXY_FIELD).getAsBoolean() : TSystem.DEFAULT_USEPROXY);
    proxyHost = ApiUtils.getValS(jsonObject.get(PROXY_HOST_FIELD), "");
    proxyPort = (jsonObject.has(PROXY_PORT_FIELD) ? jsonObject.get(PROXY_PORT_FIELD).getAsInt() : -1);

    jobLocalWorkingDir = ApiUtils.getValS(jsonObject.get(JOB_LOCAL_WORKING_DIR_FIELD), "");
    jobLocalArchiveDir = ApiUtils.getValS(jsonObject.get(JOB_LOCAL_ARCHIVE_DIR_FIELD), "");
    jobRemoteArchiveSystem = ApiUtils.getValS(jsonObject.get(JOB_REMOTE_ARCHIVE_SYSTEM_FIELD), "");
    jobRemoteArchiveDir = ApiUtils.getValS(jsonObject.get(JOB_REMOTE_ARCHIVE_DIR_FIELD), "");
    tags = ApiUtils.getValS(jsonObject.get(TAGS_FIELD), TSystem.DEFAULT_TAGS);
    notes = ApiUtils.getValS(jsonObject.get(NOTES_FIELD), TSystem.DEFAULT_NOTES);

    // Extract access credential if provided and effectiveUserId is not dynamic. This is a model.Credential object.
    Credential accessCred = null;
    if (jsonObject.has(ACCESS_CREDENTIAL_FIELD) &&  !effectiveUserId.equals(TSystem.APIUSERID_VAR))
    {
      accessCred = extractAccessCred(jsonObject);
    }

    // Extract list of supported transfer methods. This is a list of Enums. Json schema enforces allowed enum values.
    // If element is not there or the list is empty then build empty array "{}"
    var txfrMethodsArr = new ArrayList<String>();
    StringBuilder transferMethodsSB = new StringBuilder("{");
    JsonArray txfrMethodsJson = null;
    if (jsonObject.has(TRANSFER_METHODS_FIELD)) txfrMethodsJson = jsonObject.getAsJsonArray(TRANSFER_METHODS_FIELD);
    if (txfrMethodsJson != null && txfrMethodsJson.size() > 0)
    {
      for (int i = 0; i < txfrMethodsJson.size()-1; i++)
      {
        transferMethodsSB.append(txfrMethodsJson.get(i).toString()).append(",");
        txfrMethodsArr.add(StringUtils.remove(txfrMethodsJson.get(i).toString(),'"'));
      }
      transferMethodsSB.append(txfrMethodsJson.get(txfrMethodsJson.size()-1).toString());
      txfrMethodsArr.add(StringUtils.remove(txfrMethodsJson.get(txfrMethodsJson.size()-1).toString(),'"'));
    }
    transferMethodsSB.append("}");

    // Extract list of job capabilities. This is a list of Capability objects.
    var jobCaps = new ArrayList<Capability>();
    JsonArray jobCapsJson = null;
    if (jsonObject.has(JOB_CAPABILITIES_FIELD)) jobCapsJson = jsonObject.getAsJsonArray(JOB_CAPABILITIES_FIELD);
    if (jobCapsJson != null && jobCapsJson.size() > 0)
    {
      Capability cap;
      for (JsonElement jsonElement : jobCapsJson)
      {
        // Extract capability from the json object
        cap = extractCapability(systemName, jsonElement.getAsJsonObject());
        jobCaps.add(cap);
      }
    }

    // Check values. name, host, accessMetheod must be set. effectiveUserId is restricted.
    // If transfer mechanism S3 is supported then bucketName must be set.
    // Collect and report as many errors as possible so they can all be fixed before next attempt
    var errMessages = new ArrayList<String>();
    if (StringUtils.isBlank(systemName))
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty name.");
      errMessages.add(msg);
    }
    if (StringUtils.isBlank(systemType))
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty system type.");
      errMessages.add(msg);
    }
    else if (StringUtils.isBlank(host))
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty host.");
      errMessages.add(msg);
    }
    else if (StringUtils.isBlank(accessMethodStr))
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "createSystem", "Null or empty access method.");
      errMessages.add(msg);
    }
    else if (accessMethodStr.equals(AccessMethod.CERT.name()) &&
            !effectiveUserId.equals(TSystem.APIUSERID_VAR) &&
            !effectiveUserId.equals(TSystem.OWNER_VAR) &&
            !StringUtils.isBlank(owner) &&
            !effectiveUserId.equals(owner))
    {
      // For CERT access the effectiveUserId cannot be static string other than owner
      msg = ApiUtils.getMsg("SYSAPI_INVALID_EFFECTIVEUSERID_INPUT");
      errMessages.add(msg);
    }
    else if (txfrMethodsArr.contains(TransferMethod.S3.name()) && StringUtils.isBlank(bucketName))
    {
      // For S3 support bucketName must be set
      msg = ApiUtils.getMsg("SYSAPI_S3_NOBUCKET_INPUT");
      errMessages.add(msg);
    }
    else if (jsonObject.has(ACCESS_CREDENTIAL_FIELD) && effectiveUserId.equals(TSystem.APIUSERID_VAR))
    {
      // If effectiveUserId is dynamic then providing credentials is disallowed
      msg = ApiUtils.getMsg("SYSAPI_CRED_DISALLOWED_INPUT");
      errMessages.add(msg);
    }

    // If validation failed log error message and return response
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors("SYSAPI_CREATE_INVALID_ERRORLIST", errMessages);
      _log.error(allErrors);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(allErrors, prettyPrint)).build();
    }

    // Convert access method string to enum
    // If Enums defined correctly an exception should never be thrown, but just in case.
    AccessMethod accessMethod;
    try { accessMethod =  AccessMethod.valueOf(accessMethodStr); }
    catch (IllegalArgumentException e)
    {
      msg = ApiUtils.getMsg("SYSAPI_ACCMETHOD_ENUM_ERROR", accessMethodStr, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }


    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = maskCredSecrets(jsonObject);

    // Make the service call to create the system
    systemsService = new SystemsServiceImpl();
    try
    {
      systemsService.createSystem(tenantName, apiUserId, systemName, description, systemType, owner, host, available,
                                  effectiveUserId, accessMethod, accessCred,
                                  bucketName, rootDir, transferMethodsSB.toString(),
                                  port, useProxy, proxyHost, proxyPort,
                                  jobCanExec, jobLocalWorkingDir, jobLocalArchiveDir, jobRemoteArchiveSystem,
                                  jobRemoteArchiveDir, jobCaps, tags, notes, scrubbedJson);
    }
    catch (IllegalStateException e)
    {
      // IllegalStateException indicates object exists - return 409 - Conflict
      msg = ApiUtils.getMsg("SYSAPI_SYS_EXISTS", systemName);
      _log.warn(msg);
      return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_ERROR", systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemName;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
      ApiUtils.getMsg("SYSAPI_CREATED", systemName), prettyPrint, resp1)).build();
  }

  /**
   * getSystemByName
   * @param sysName - name of the system
   * @param getCreds - should credentials be included
   * @param accessMethodStr - access method to use instead of default
   * @param prettyPrint - pretty print the output
   * @return Response with system object as the result
   */
  @GET
  @Path("{sysName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Retrieve information for a system",
      description =
          "Retrieve information for a system given the system name. " +
          "Use query parameter returnCredentials=true to have the user access credentials " +
          "included in the response. " +
          "Use query parameter accessMethod=<method> to override default access method.",
      tags = "systems",
// TODO Including parameter info here and in method sig results in duplicates in openapi spec.
// TODO    JAX-RS appears to require the annotations in the method sig
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
  public Response getSystemByName(@PathParam("sysName") String sysName,
                                  @QueryParam("returnCredentials") @DefaultValue("false") boolean getCreds,
                                  @QueryParam("accessMethod") @DefaultValue("") String accessMethodStr,
                                  @QueryParam("pretty") @DefaultValue("false") boolean prettyPrint)
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

    // Check that accessMethodStr is valid if is passed in
    AccessMethod accessMethod = null;
    try { if (!StringUtils.isBlank(accessMethodStr)) accessMethod =  AccessMethod.valueOf(accessMethodStr); }
    catch (IllegalArgumentException e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_ACCMETHOD_ENUM_ERROR", accessMethodStr, sysName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    TSystem system;
    try
    {
      system = systemsService.getSystemByName(tenant, sysName, apiUserId, getCreds, accessMethod);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_GET_NAME_ERROR", sysName, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Resource was not found.
    if (system == null)
    {
      String msg = ApiUtils.getMsg("SYSAPI_NOT_FOUND", sysName);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSystem resp1 = new RespSystem(system);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
        MsgUtils.getMsg("TAPIS_FOUND", "System", sysName), prettyPrint, resp1)).build();
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
      String msg = ApiUtils.getMsg("SYSAPI_SELECT_ERROR", e.getMessage());
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
   * @param sysName - name of the system to delete
   * @param prettyPrint - pretty print the output
   * @return - response with change count as the result
   */
  @DELETE
  @Path("{sysName}")
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
  public Response deleteSystemByName(@PathParam("sysName") String sysName,
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
      changeCount = systemsService.deleteSystemByName(tenant, sysName);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsg("SYSAPI_DELETE_NAME_ERROR", sysName, e.getMessage());
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
      MsgUtils.getMsg("TAPIS_DELETED", "System", sysName), prettyPrint, resp1)).build();
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Extract Capability from a Json object
   * @param obj Json object from request
   * @return A Capability object
   */
  private static Capability extractCapability(String systemName, JsonObject obj)
  {
    String name, value;
    Category category = null;
    String categoryStr = ApiUtils.getValS(obj.get(JOB_CAPABILITY_CATEGORY_FIELD), "");
    if (!StringUtils.isBlank(categoryStr))
    {
      // If Enums defined correctly an exception should never be thrown, but just in case.
      try { category =  Category.valueOf(categoryStr); }
      catch (IllegalArgumentException e)
      {
        String msg = ApiUtils.getMsg("SYSAPI_CAP_ENUM_ERROR", categoryStr, systemName, e.getMessage());
        _log.error(msg, e);
        return null;
      }
    }
    name = ApiUtils.getValS(obj.get(JOB_CAPABILITY_NAME_FIELD), "");
    value = ApiUtils.getValS(obj.get(JOB_CAPABILITY_VALUE_FIELD), "");
    return new Capability(category, name, value);
  }

  /**
   * Extract AccessCredential details from the top level Json object
   * @param obj Top level Json object from request
   * @return A partially populated Credential object
   */
  private static Credential extractAccessCred(JsonObject obj)
  {
    String password, privateKey, publicKey, sshCert, accessKey, accessSecret;
    JsonObject credObj = obj.getAsJsonObject(ACCESS_CREDENTIAL_FIELD);
    password = ApiUtils.getValS(credObj.get(CredentialResource.PASSWORD_FIELD), "");
    privateKey = ApiUtils.getValS(credObj.get(CredentialResource.PRIVATE_KEY_FIELD), "");
    publicKey = ApiUtils.getValS(credObj.get(CredentialResource.PUBLIC_KEY_FIELD), "");
    sshCert = ApiUtils.getValS(credObj.get(CredentialResource.CERTIFICATE_FIELD), "");
    accessKey = ApiUtils.getValS(credObj.get(CredentialResource.ACCESS_KEY_FIELD), "");
    accessSecret = ApiUtils.getValS(credObj.get(CredentialResource.ACCESS_SECRET_FIELD), "");
    return new Credential(password, privateKey, publicKey, sshCert, accessKey, accessSecret);
  }

  /**
   * AccessCredential details can contain secrets. Mask any secrets given
   * and return a string containing the final Json.
   * @param obj1 Top level Json object from request
   * @return A string with any secrets masked out
   */
  private static String maskCredSecrets(JsonObject obj1)
  {
    if (!obj1.has(ACCESS_CREDENTIAL_FIELD)) return obj1.toString();
    // Leave the incoming object alone
    var obj2 = obj1.deepCopy();
    var credObj = obj2.getAsJsonObject(ACCESS_CREDENTIAL_FIELD);
    maskSecret(credObj, CredentialResource.PASSWORD_FIELD);
    maskSecret(credObj, CredentialResource.PRIVATE_KEY_FIELD);
    maskSecret(credObj, CredentialResource.PUBLIC_KEY_FIELD);
    maskSecret(credObj, CredentialResource.CERTIFICATE_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_SECRET_FIELD);
    obj2.remove(ACCESS_CREDENTIAL_FIELD);
    obj2.add(ACCESS_CREDENTIAL_FIELD, credObj);
    return obj2.toString();
  }

  /**
   * If the Json object contains a non-blank value for the field then replace the value with the mask value.
   */
  private static void maskSecret(JsonObject credObj, String field)
  {
    if (!StringUtils.isBlank(ApiUtils.getValS(credObj.get(field), "")))
    {
      credObj.remove(field);
      credObj.addProperty(field, SECRETS_MASK);
    }
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(String firstLineKey, List<String> msgList) {
    if (StringUtils.isBlank(firstLineKey) || msgList == null || msgList.isEmpty()) return "";
    var sb = new StringBuilder(ApiUtils.getMsg(firstLineKey));
    sb.append(System.lineSeparator());
    for (String msg : msgList)
    {
      sb.append("  ").append(msg).append(System.lineSeparator());
    }
    return sb.toString();
  }
}

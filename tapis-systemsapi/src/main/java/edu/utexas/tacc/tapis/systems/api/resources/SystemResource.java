package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
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

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.api.requests.ReqImportSGCIResource;
import edu.utexas.tacc.tapis.systems.api.requests.ReqUpdateSGCISystem;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemArray;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.systems.api.requests.ReqCreateSystem;
import edu.utexas.tacc.tapis.systems.api.requests.ReqSearchSystems;
import edu.utexas.tacc.tapis.systems.api.requests.ReqUpdateSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.service.SystemsService;

/*
 * JAX-RS REST resource for a Tapis System (edu.utexas.tacc.tapis.systems.model.TSystem)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see tapis-systemsapi/src/main/resources/SystemsAPI.yaml
 *       and note at top of SystemsResource.java
 *
 * NOTE: The "pretty" query parameter is available for all endpoints. It is processed in
 *       QueryParametersRequestFilter.java.
 */
@Path("/v3/systems")
public class SystemResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemResource.class);

  // Json schema resource files.
  private static final String FILE_SYSTEM_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemCreateRequest.json";
  private static final String FILE_SYSTEM_UPDATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemUpdateRequest.json";
  private static final String FILE_SYSTEM_SEARCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemSearchRequest.json";
  private static final String FILE_SYSTEM_IMPORTSGCI_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemImportSGCIRequest.json";
  private static final String FILE_SYSTEM_UPDATESGCI_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemUpdateSGCIRequest.json";

  // Field names used in Json
  private static final String NAME_FIELD = "name";
  private static final String NOTES_FIELD = "notes";
  private static final String SYSTEM_TYPE_FIELD = "systemType";
  private static final String HOST_FIELD = "host";
  private static final String DEFAULT_ACCESS_METHOD_FIELD = "defaultAccessMethod";
  private static final String ACCESS_CREDENTIAL_FIELD = "accessCredential";
  private static final String SEARCH_FIELD = "search";

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
  private Request _request;

  // **************** Inject Services using HK2 ****************
  @Inject
  private SystemsService systemsService;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Create a system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to created object
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
//  @Operation(
//    summary = "Create a system",
//    description =
//        "Create a system using a request body. " +
//        "System name must be unique within a tenant and can be composed of alphanumeric characters " +
//        "and the following special characters: [-._~]. Name must begin with an alphabetic character " +
//        "and can be no more than 256 characters in length. " +
//        "Description is optional with a maximum length of 2048 characters.",
//    tags = "systems",
////    parameters = {
////      @Parameter(name = "pretty", description = "Pretty print the response", in = ParameterIn.QUERY, schema = @Schema(type = "boolean"))
////    },
//    requestBody =
//      @RequestBody(
//        description = "A JSON object specifying information for the system to be created.",
//        required = true,
//        content = @Content(schema = @Schema(implementation = ReqCreateSystem.class))
//      ),
//    responses = {
//      @ApiResponse(responseCode = "201", description = "System created.",
//                   content = @Content(schema = @Schema(implementation = RespResourceUrl.class))),
//      @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "401", description = "Not authorized.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "409", description = "System already exists.",
//                   content = @Content(schema = @Schema(implementation = RespResourceUrl.class))),
//      @ApiResponse(responseCode = "500", description = "Server error.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
//    }
//  )
  public Response createSystem(InputStream payloadStream,
                               @Context SecurityContext securityContext)
  {
    String opName = "createSystem";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson, msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName , e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_CREATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    ReqCreateSystem req;
    // ------------------------- Create a TSystem from the json and validate constraints -------------------------
    try {
      req = TapisGsonUtils.getGson().fromJson(rawJson, ReqCreateSystem.class);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create a TSystem from the request
    TSystem system = createTSystemFromRequest(req);
    // Fill in defaults and check constraints on TSystem attributes
    resp = validateTSystem(system, authenticatedUser, prettyPrint);
    if (resp != null) return resp;

    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);
    system.setNotes(notes);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (system.getAccessCredential() != null) scrubbedJson = maskCredSecrets(rawJson);

    // ---------------------------- Make service call to create the system -------------------------------
    // Update tenant name and pull out system name for convenience
    system.setTenant(authenticatedUser.getTenantId());
    String systemName = system.getName();
    try
    {
      systemsService.createSystem(authenticatedUser, system, scrubbedJson);
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_SYS_EXISTS"))
      {
        // IllegalStateException with msg containing SYS_EXISTS indicates object exists - return 409 - Conflict
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_EXISTS", authenticatedUser, systemName);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemName, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid TSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemName;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
      ApiUtils.getMsgAuth("SYSAPI_CREATED", authenticatedUser, systemName), prettyPrint, resp1)).build();
  }

  /**
   * Update a system
   * @param systemName - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @PATCH
  @Path("{systemName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
//  @Operation(
//          summary = "Update a system",
//          description =
//                  "Update attributes for a system. Only certain attributes may be updated: " +
//                  "description, host, enabled, effectiveUserId, defaultAccessMethod, transferMethods, " +
//                  "port, useProxy, proxyHost, proxyPort, jobCapabilities, tags, notes.",
//          tags = "systems",
//          requestBody =
//          @RequestBody(
//                  description = "A JSON object specifying changes to be applied.",
//                  required = true,
//                  content = @Content(schema = @Schema(implementation = ReqUpdateSystem.class))
//          ),
//          responses = {
//                  @ApiResponse(responseCode = "200", description = "System updated.",
//                          content = @Content(schema = @Schema(implementation = RespResourceUrl.class))),
//                  @ApiResponse(responseCode = "400", description = "Input error. Invalid JSON.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//                  @ApiResponse(responseCode = "401", description = "Not authorized.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//                  @ApiResponse(responseCode = "404", description = "System not found.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//                  @ApiResponse(responseCode = "500", description = "Server error.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
//          }
//  )
  public Response updateSystem(@PathParam("systemName") String systemName,
                               InputStream payloadStream,
                               @Context SecurityContext securityContext)
  {
    String opName = "updateSystem";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson, msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName , e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_UPDATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ------------------------- Create a PatchSystem from the json and validate constraints -------------------------
    ReqUpdateSystem req;
    try {
      req = TapisGsonUtils.getGson().fromJson(rawJson, ReqUpdateSystem.class);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    PatchSystem patchSystem = createPatchSystemFromRequest(req, authenticatedUser.getTenantId(), systemName);

    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);
    patchSystem.setNotes(notes);

    // No attributes are required. Constraints validated and defaults filled in on server side.
    // No secrets in PatchSystem so no need to scrub

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      systemsService.updateSystem(authenticatedUser, patchSystem, rawJson);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemName);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemName, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_UPDATED", authenticatedUser, systemName), prettyPrint, resp1)).build();
  }

  /**
   * Import a system - create a system based on an attributes from an external source
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to created object
   */
  @POST
  @Path("import/sgci")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response importSGCIResource(InputStream payloadStream,
                                     @Context SecurityContext securityContext)
  {
    String opName = "importSGCIResource";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson, msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName , e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_IMPORTSGCI_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    ReqImportSGCIResource req;
    // ------------------------- Create a TSystem from the json and validate constraints -------------------------
    try {
      req = TapisGsonUtils.getGson().fromJson(rawJson, ReqImportSGCIResource.class);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // TODO ???????????????????????????????????????????????????????????????
    if (true)
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse("WIP: COMING SOON", prettyPrint)).build();

    // Construct the name or get it from the request
    String systemName;
    if (StringUtils.isBlank(req.name))
    {
      systemName = "sgci-" + req.sgciResourceId;
    }
    else
    {
      systemName = req.name;
    }
    // TODO Create a TSystem from the request
    TSystem system = null; //createTSystemFromSGCIImportRequest(req, systemName);
    system.setImportRefId(req.sgciResourceId);
    // Fill in defaults and check constraints on TSystem attributes
    resp = validateTSystem(system, authenticatedUser, prettyPrint);
    if (resp != null) return resp;

    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);
    system.setNotes(notes);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (system.getAccessCredential() != null) scrubbedJson = maskCredSecrets(rawJson);

    // ---------------------------- Make service call to create the system -------------------------------
    // Update tenant name and pull out system name for convenience
    system.setTenant(authenticatedUser.getTenantId());
    try
    {
      systemsService.createSystem(authenticatedUser, system, scrubbedJson);
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_SYS_EXISTS"))
      {
        // IllegalStateException with msg containing SYS_EXISTS indicates object exists - return 409 - Conflict
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_EXISTS", authenticatedUser, systemName);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemName, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid TSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemName;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_CREATED", authenticatedUser, systemName), prettyPrint, resp1)).build();
  }

  /**
   * Update a system that was created based on an SGCI resource
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @PATCH
  @Path("import/sgci/{systemName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateSGCISystem(@PathParam("systemName") String systemName,
                                   InputStream payloadStream,
                                   @Context SecurityContext securityContext)
  {
    String opName = "updateSGCISystem";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson, msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName , e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_UPDATESGCI_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ------------------------- Create a PatchSystem from the json and validate constraints -------------------------
    ReqUpdateSGCISystem req;
    try {
      req = TapisGsonUtils.getGson().fromJson(rawJson, ReqUpdateSGCISystem.class);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // TODO ???????????????????????????????????????????????????????????????
    if (true)
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse("WIP: COMING SOON", prettyPrint)).build();

    // TODO
    PatchSystem patchSystem = null;// createPatchSystemFromSGCIRequest(req, authenticatedUser.getTenantId(), systemName);

    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);
    patchSystem.setNotes(notes);

    // No attributes are required. Constraints validated and defaults filled in on server side.
    // No secrets in PatchSystem so no need to scrub

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      systemsService.updateSystem(authenticatedUser, patchSystem, rawJson);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemName);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemName, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_UPDATED", authenticatedUser, systemName), prettyPrint, resp1)).build();
  }

  /**
   * Change owner of a system
   * @param systemName - name of the system
   * @param userName - name of the new owner
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @POST
  @Path("{systemName}/changeOwner/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
//  @Operation(
//          summary = "Change owner of a system",
//          description =
//                  "Change owner of a system.",
//          tags = "systems",
//          responses = {
//                  @ApiResponse(responseCode = "200", description = "System owner updated.",
//                          content = @Content(schema = @Schema(implementation = RespChangeCount.class))),
//                  @ApiResponse(responseCode = "401", description = "Not authorized.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//                  @ApiResponse(responseCode = "404", description = "System not found.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//                  @ApiResponse(responseCode = "500", description = "Server error.",
//                          content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
//          }
//  )
  public Response changeSystemOwner(@PathParam("systemName") String systemName,
                                    @PathParam("userName") String userName,
                                    @Context SecurityContext securityContext)
  {
    String opName = "changeSystemOwner";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // ---------------------------- Make service call to update the system -------------------------------
    int changeCount;
    String msg;
    try
    {
      changeCount = systemsService.changeSystemOwner(authenticatedUser, systemName, userName);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemName);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemName, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    // Return the number of objects impacted.
    ResultChangeCount count = new ResultChangeCount();
    count.changes = changeCount;
    RespChangeCount resp1 = new RespChangeCount(count);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_UPDATED", authenticatedUser, systemName), prettyPrint, resp1)).build();
  }

  /**
   * getSystemByName
   * @param systemName - name of the system
   * @param getCreds - should credentials of effectiveUser be included
   * @param accessMethodStr - access method to use instead of default
   * @param securityContext - user identity
   * @return Response with system object as the result
   */
  @GET
  @Path("{systemName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
//  @Operation(
//      summary = "Retrieve information for a system",
//      description =
//          "Retrieve information for a system given the system name. " +
//          "Use query parameter returnCredentials=true to have effectiveUserId access credentials " +
//          "included in the response. " +
//          "Use query parameter accessMethod=<method> to override default access method.",
//      tags = "systems",
//      parameters = {
//        @Parameter(name = "select", description = "Resource attributes to include when returning results. For example select=result.name,result.host",
//                   in = ParameterIn.QUERY, schema = @Schema(type = "string"))
//      },
//      responses = {
//          @ApiResponse(responseCode = "200", description = "System found.",
//            content = @Content(schema = @Schema(implementation = RespSystem.class))),
//          @ApiResponse(responseCode = "400", description = "Input error.",
//            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//          @ApiResponse(responseCode = "404", description = "System not found.",
//            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//          @ApiResponse(responseCode = "401", description = "Not authorized.",
//            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//          @ApiResponse(responseCode = "500", description = "Server error.",
//            content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
//      }
//  )
  public Response getSystemByName(@PathParam("systemName") String systemName,
                                  @QueryParam("returnCredentials") @DefaultValue("false") boolean getCreds,
                                  @QueryParam("accessMethod") @DefaultValue("") String accessMethodStr,
                                  @Context SecurityContext securityContext)
  {
    String opName = "getSystemByName";
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // Check that accessMethodStr is valid if is passed in
    AccessMethod accessMethod = null;
    try { if (!StringUtils.isBlank(accessMethodStr)) accessMethod =  AccessMethod.valueOf(accessMethodStr); }
    catch (IllegalArgumentException e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_ACCMETHOD_ENUM_ERROR", authenticatedUser, systemName, accessMethodStr, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    TSystem system;
    try
    {
      system = systemsService.getSystemByName(authenticatedUser, systemName, getCreds, accessMethod);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_GET_NAME_ERROR", authenticatedUser, systemName, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Resource was not found.
    if (system == null)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemName);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSystem resp1 = new RespSystem(system);
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "System", systemName), resp1);
  }

  /**
   * getSystems
   * Retrieve all systems accessible by requester and matching any search conditions provided as a single
   * search query parameter.
   * @param searchStr -  List of strings indicating search conditions to use when retrieving results
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
//  @Operation(
//    summary = "Retrieve list of systems",
//    description = "Retrieve list of systems.",
//    tags = "systems",
//// TODO/TBD: In order to have select and search as parameters in the client we need to use @QueryParam
////           but having it also in "parameters=" causes problems with the generated openapi.
////  TBD: it causes duplicate entries in openapi.json, the file is then invalid json.
////    parameters = {
////      @Parameter(name = "select", description = "Resource attributes to include when returning results. " +
////                                                "For example select=result.name,result.host",
////                 in = ParameterIn.QUERY, schema = @Schema(type = "string")),
////      @Parameter(name = "search", description = "Search conditions to use when retrieving results. " +
////                                                "For example, search=name.eq.Lsystem1,enabled.eq.true",
////                 in = ParameterIn.QUERY, schema = @Schema(type = "string"))
////    },
//    responses = {
//      @ApiResponse(responseCode = "200", description = "Success.",
//                   content = @Content(schema = @Schema(implementation = RespSystemArray.class))),
//      @ApiResponse(responseCode = "400", description = "Input error.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "401", description = "Not authorized.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "500", description = "Server error.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
//    }
//  )
  public Response getSystems(@QueryParam("search") String searchStr,
                             @Context SecurityContext securityContext)
  {
    String opName = "getSystems";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    List<String> searchList = null;
    try
    {
      // Extract the search conditions and validate their form. Back end will handle translating LIKE wildcard
      //   characters (* and !) and dealing with special characters in values.
      searchList = SearchUtils.extractAndValidateSearchList(searchStr);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SEARCH_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (searchList != null && !searchList.isEmpty()) _log.debug("Using searchList. First value = " + searchList.get(0));

    // ------------------------- Retrieve all records -----------------------------
    List<TSystem> systems;
    try { systems = systemsService.getSystems(authenticatedUser, searchList); }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    if (systems == null) systems = Collections.emptyList();
    int cnt = systems.size();
    RespSystemArray resp1 = new RespSystemArray(systems);
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", cnt + " items"), resp1);
  }

  /**
   * searchSystemsQueryParameters
   * Dedicated search endpoint for System resource. Search conditions provided as query parameters.
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Path("search/systems")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsQueryParameters(@Context SecurityContext securityContext)
  {
    String opName = "searchSystemsGet";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // Create search list based on query parameters
    // Note that some validation is done for each condition but the back end will handle translating LIKE wildcard
    //   characters (* and !) and deal with escaped characters.
    List<String> searchList;
    try
    {
      searchList = SearchUtils.buildListFromQueryParms(_uriInfo.getQueryParameters());
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SEARCH_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (searchList != null && !searchList.isEmpty()) _log.debug("Using searchList. First value = " + searchList.get(0));

    // ------------------------- Retrieve all records -----------------------------
    List<TSystem> systems;
    try { systems = systemsService.getSystems(authenticatedUser, searchList); }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    if (systems == null) systems = Collections.emptyList();
    int cnt = systems.size();
    RespSystemArray resp1 = new RespSystemArray(systems);
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", cnt + " items"), resp1);
  }

  /**
   * searchSystemsRequestBody
   * Dedicated search endpoint for System resource. Search conditions provided in a request body.
   * Request body contains an array of strings that are concatenated to form the full SQL-like search string.
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @POST
  @Path("search/systems")
//  @Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsRequestBody(InputStream payloadStream,
                                           @Context SecurityContext securityContext)
  {
    String opName = "searchSystemsPost";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson, msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName , e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_SEARCH_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Create array of search strings form the json object
    ReqSearchSystems req;
    try {
      req = TapisGsonUtils.getGson().fromJson(rawJson, ReqSearchSystems.class);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // Concatenate all strings into a single SQL-like search string
    // When put together full string must be a valid SQL-like where clause. This will be validated in the service call.
    // Not all SQL syntax is supported. See SqlParser.jj in tapis-shared-searchlib.
    StringJoiner sj = new StringJoiner(" ");
    for (String s : req.search) { sj.add(s); }
    String searchStr = sj.toString();
    _log.debug("Using search string: " + searchStr);

    // ------------------------- Retrieve all records -----------------------------
    List<TSystem> systems;
    try { systems = systemsService.getSystemsUsingSqlSearchStr(authenticatedUser, searchStr); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    if (systems == null) systems = Collections.emptyList();
    int cnt = systems.size();
    RespSystemArray resp1 = new RespSystemArray(systems);
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", cnt + " items"), resp1);
  }

  /**
   * deleteSystemByName
   * @param systemName - name of the system to delete
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @DELETE
  @Path("{systemName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
//  @Operation(
//    summary = "Soft delete a system given the system name",
//    description = "Soft delete a system given the system name. ",
//    tags = "systems",
////    parameters = {
////      @Parameter(name = "pretty", description = "Pretty print the response", in = ParameterIn.QUERY, schema = @Schema(type = "boolean"))
////    },
//    responses = {
//      @ApiResponse(responseCode = "200", description = "System deleted.",
//        content = @Content(schema = @Schema(implementation = RespChangeCount.class))),
//      @ApiResponse(responseCode = "400", description = "Input error.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "401", description = "Not authorized.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "500", description = "Server error.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))
//    }
//  )
// TODO Add query parameter "confirm" which must be set to true since this is an operation that cannot be undone by a user
  public Response deleteSystemByName(@PathParam("systemName") String systemName,
                                     @Context SecurityContext securityContext)
  {
    String opName = "deleteSystemByName";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    int changeCount;
    try
    {
      changeCount = systemsService.softDeleteSystemByName(authenticatedUser, systemName);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_DELETE_NAME_ERROR", authenticatedUser, systemName, e.getMessage());
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
      MsgUtils.getMsg("TAPIS_DELETED", "System", systemName), prettyPrint, resp1)).build();
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Create a TSystem from a ReqCreateSystem
   */
  private static TSystem createTSystemFromRequest(ReqCreateSystem req)
  {
    var system = new TSystem(-1, null, req.name, req.description, req.systemType, req.owner, req.host,
                       req.enabled, req.effectiveUserId, req.defaultAccessMethod,
                       req.bucketName, req.rootDir, req.transferMethods, req.port, req.useProxy,
                       req.proxyHost, req.proxyPort, req.jobCanExec, req.jobLocalWorkingDir,
                       req.jobLocalArchiveDir, req.jobRemoteArchiveSystem, req.jobRemoteArchiveDir,
                       req.tags, req.notes, req.refImportId, false, null, null);
    system.setAccessCredential(req.accessCredential);
    system.setJobCapabilities(req.jobCapabilities);
    return system;
  }

  /**
   * Create a PatchSystem from a ReqUpdateSystem
   */
  private static PatchSystem createPatchSystemFromRequest(ReqUpdateSystem req, String tenantName, String systemName)
  {
    PatchSystem patchSystem = new PatchSystem(req.description, req.host, req.enabled, req.effectiveUserId,
                           req.defaultAccessMethod, req.transferMethods, req.port, req.useProxy,
                           req.proxyHost, req.proxyPort, req.jobCapabilities, req.tags, req.notes);
    // Update tenant name and system name
    patchSystem.setTenant(tenantName);
    patchSystem.setName(systemName);
    return patchSystem;
  }

  /**
   * Fill in defaults and check constraints on TSystem attributes
   * Check values. name, host, accessMethod must be set. effectiveUserId is restricted.
   * If transfer mechanism S3 is supported then bucketName must be set.
   * Collect and report as many errors as possible so they can all be fixed before next attempt
   * NOTE: JsonSchema validation should handle some of these checks but we check here again just in case
   *
   * @return null if OK or error Response
   */
  private static Response validateTSystem(TSystem system, AuthenticatedUser authenticatedUser, boolean prettyPrint)
  {
    // Make sure owner, effectiveUserId, transferMethods, notes and tags are all set
    TSystem system1 = TSystem.checkAndSetDefaults(system);

    String effectiveUserId = system1.getEffectiveUserId();
    String owner  = system1.getOwner();
    String name = system1.getName();
    String msg;
    var errMessages = new ArrayList<String>();
    if (StringUtils.isBlank(system1.getName()))
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_MISSING_ATTR", NAME_FIELD);
      errMessages.add(msg);
    }
    if (system1.getSystemType() == null)
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_MISSING_ATTR", SYSTEM_TYPE_FIELD);
      errMessages.add(msg);
    }
    if (StringUtils.isBlank(system1.getHost()))
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_MISSING_ATTR", HOST_FIELD);
      errMessages.add(msg);
    }
    if (system1.getDefaultAccessMethod() == null)
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_MISSING_ATTR", DEFAULT_ACCESS_METHOD_FIELD);
      errMessages.add(msg);
    }
    if (system1.getDefaultAccessMethod().equals(AccessMethod.CERT) &&
            !effectiveUserId.equals(TSystem.APIUSERID_VAR) &&
            !effectiveUserId.equals(TSystem.OWNER_VAR) &&
            !StringUtils.isBlank(owner) &&
            !effectiveUserId.equals(owner))
    {
      // For CERT access the effectiveUserId cannot be static string other than owner
      msg = ApiUtils.getMsg("SYSAPI_INVALID_EFFECTIVEUSERID_INPUT");
      errMessages.add(msg);
    }
    if (system1.getTransferMethods() != null &&
            system1.getTransferMethods().contains(TransferMethod.S3) && StringUtils.isBlank(system1.getBucketName()))
    {
      // For S3 support bucketName must be set
      msg = ApiUtils.getMsg("SYSAPI_S3_NOBUCKET_INPUT");
      errMessages.add(msg);
    }
    if (system1.getAccessCredential() != null && effectiveUserId.equals(TSystem.APIUSERID_VAR))
    {
      // If effectiveUserId is dynamic then providing credentials is disallowed
      msg = ApiUtils.getMsg("SYSAPI_CRED_DISALLOWED_INPUT");
      errMessages.add(msg);
    }

    // If validation failed log error message and return response
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(errMessages, authenticatedUser, name);
      _log.error(allErrors);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(allErrors, prettyPrint)).build();
    }
    return null;
  }

  /**
   * Extract notes from the incoming json
   */
  private static Object extractNotes(String rawJson)
  {
    Object notes = TSystem.DEFAULT_NOTES;
    // Check inputs
    if (StringUtils.isBlank(rawJson)) return notes;
    // Turn the request string into a json object and extract the notes object
    JsonObject topObj = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);
    if (!topObj.has(NOTES_FIELD)) return notes;
    notes = topObj.getAsJsonObject(NOTES_FIELD);
    return notes;
  }

  /**
   * AccessCredential details can contain secrets. Mask any secrets given
   * and return a string containing the final redacted Json.
   * @param rawJson Json from request
   * @return A string with any secrets masked out
   */
  private static String maskCredSecrets(String rawJson)
  {
    if (StringUtils.isBlank(rawJson)) return rawJson;
    // Get the Json object and prepare to extract info from it
    JsonObject sysObj = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);
    if (!sysObj.has(ACCESS_CREDENTIAL_FIELD)) return rawJson;
    var credObj = sysObj.getAsJsonObject(ACCESS_CREDENTIAL_FIELD);
    maskSecret(credObj, CredentialResource.PASSWORD_FIELD);
    maskSecret(credObj, CredentialResource.PRIVATE_KEY_FIELD);
    maskSecret(credObj, CredentialResource.PUBLIC_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_SECRET_FIELD);
    maskSecret(credObj, CredentialResource.CERTIFICATE_FIELD);
    sysObj.remove(ACCESS_CREDENTIAL_FIELD);
    sysObj.add(ACCESS_CREDENTIAL_FIELD, credObj);
    return sysObj.toString();
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
  private static String getListOfErrors(List<String> msgList, AuthenticatedUser authenticatedUser, Object... parms) {
    if (msgList == null || msgList.isEmpty()) return "";
    var sb = new StringBuilder(ApiUtils.getMsgAuth("SYSAPI_CREATE_INVALID_ERRORLIST", authenticatedUser, parms));
    sb.append(System.lineSeparator());
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  private void logRequest(String opName) {
    String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), opName,
            "  " + _request.getRequestURL());
    _log.trace(msg);
  }

  /**
   * Create an OK response given message and base response to put in result
   * @param msg - message for resp.message
   * @param resp - base response (the result)
   * @return - Final response to return to client
   */
  private static Response createSuccessResponse(String msg, RespAbstract resp)
  {
    resp.message = msg;
    resp.status = ResponseWrapper.RESPONSE_STATUS.success.name();
    resp.version = TapisUtils.getTapisVersion();
    return Response.ok(resp).build();
  }
}

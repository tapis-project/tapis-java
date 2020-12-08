package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import edu.utexas.tacc.tapis.systems.api.requests.ReqUpdateSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemsSearch;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemsArray;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.service.SystemsService;

/*
 * JAX-RS REST resource for a Tapis System (edu.utexas.tacc.tapis.systems.model.TSystem)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see tapis-client-java repo file systems-client/SystemsAPI.yaml
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
  private static final String FILE_SYSTEM_MATCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/MatchConstraintsRequest.json";
  private static final String FILE_SYSTEM_IMPORTSGCI_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemImportSGCIRequest.json";
  private static final String FILE_SYSTEM_UPDATESGCI_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemUpdateSGCIRequest.json";

  // Field names used in Json
  private static final String ID_FIELD = "id";
  private static final String NOTES_FIELD = "notes";
  private static final String SYSTEM_TYPE_FIELD = "systemType";
  private static final String HOST_FIELD = "host";
  private static final String DEFAULT_AUTHN_METHOD_FIELD = "defaultAuthnMethod";
  private static final String AUTHN_CREDENTIAL_FIELD = "authnCredential";

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
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
    if (system.getAuthnCredential() != null) scrubbedJson = maskCredSecrets(rawJson);

    // ---------------------------- Make service call to create the system -------------------------------
    // Update tenant name and pull out system name for convenience
    system.setTenant(authenticatedUser.getTenantId());
    String systemId = system.getId();
    try
    {
      systemsService.createSystem(authenticatedUser, system, scrubbedJson);
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_SYS_EXISTS"))
      {
        // IllegalStateException with msg containing SYS_EXISTS indicates object exists - return 409 - Conflict
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_EXISTS", authenticatedUser, systemId);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid TSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemId, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemId;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
      ApiUtils.getMsgAuth("SYSAPI_CREATED", authenticatedUser, systemId), prettyPrint, resp1)).build();
  }

  /**
   * Update a system
   * @param systemId - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @PATCH
  @Path("{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateSystem(@PathParam("systemId") String systemId,
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
    PatchSystem patchSystem = createPatchSystemFromRequest(req, authenticatedUser.getTenantId(), systemId);

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
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_UPDATED", authenticatedUser, systemId), prettyPrint, resp1)).build();
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
    String systemId;
    if (StringUtils.isBlank(req.id))
    {
      systemId = "sgci-" + req.sgciResourceId;
    }
    else
    {
      systemId = req.id;
    }
    // TODO Create a TSystem from the request
    TSystem system = null; //createTSystemFromSGCIImportRequest(req, systemId);
    system.setImportRefId(req.sgciResourceId);
    // Fill in defaults and check constraints on TSystem attributes
    resp = validateTSystem(system, authenticatedUser, prettyPrint);
    if (resp != null) return resp;

    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);
    system.setNotes(notes);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (system.getAuthnCredential() != null) scrubbedJson = maskCredSecrets(rawJson);

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
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_EXISTS", authenticatedUser, systemId);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid TSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemId, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemId;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_CREATED", authenticatedUser, systemId), prettyPrint, resp1)).build();
  }

  /**
   * Update a system that was created based on an SGCI resource
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @PATCH
  @Path("import/sgci/{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateSGCISystem(@PathParam("systemId") String systemId,
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
    PatchSystem patchSystem = null;// createPatchSystemFromSGCIRequest(req, authenticatedUser.getTenantId(), systemId);

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
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_UPDATED", authenticatedUser, systemId), prettyPrint, resp1)).build();
  }

  /**
   * Change owner of a system
   * @param systemId - name of the system
   * @param userName - name of the new owner
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @POST
  @Path("{systemId}/changeOwner/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response changeSystemOwner(@PathParam("systemId") String systemId,
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
      changeCount = systemsService.changeSystemOwner(authenticatedUser, systemId, userName);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", authenticatedUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", authenticatedUser, systemId, e.getMessage());
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
            ApiUtils.getMsgAuth("SYSAPI_UPDATED", authenticatedUser, systemId), prettyPrint, resp1)).build();
  }

  /**
   * getSystem
   * @param systemId - name of the system
   * @param getCreds - should credentials of effectiveUser be included
   * @param authnMethodStr - authn method to use instead of default
   * @param requireExecPerm - check for EXECUTE permission as well as READ permission
   * @param securityContext - user identity
   * @return Response with system object as the result
   */
  @GET
  @Path("{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystem(@PathParam("systemId") String systemId,
                            @QueryParam("returnCredentials") @DefaultValue("false") boolean getCreds,
                            @QueryParam("authnMethod") @DefaultValue("") String authnMethodStr,
                            @QueryParam("requireExecPerm") @DefaultValue("false") boolean requireExecPerm,
                            @Context SecurityContext securityContext)
  {
    String opName = "getSystem";
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    // Check that authnMethodStr is valid if is passed in
    AuthnMethod authnMethod = null;
    try { if (!StringUtils.isBlank(authnMethodStr)) authnMethod =  AuthnMethod.valueOf(authnMethodStr); }
    catch (IllegalArgumentException e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_ACCMETHOD_ENUM_ERROR", authenticatedUser, systemId, authnMethodStr, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    TSystem system;
    try
    {
      system = systemsService.getSystem(authenticatedUser, systemId, getCreds, authnMethod, requireExecPerm);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_GET_NAME_ERROR", authenticatedUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Resource was not found.
    if (system == null)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", authenticatedUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSystem resp1 = new RespSystem(system);
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "System", systemId), resp1);
  }

  /**
   * getSystems
   * Retrieve all systems accessible by requester and matching any search conditions provided.
   * NOTE: The query parameters pretty, search, limit, sortBy, skip, startAfter are all handled in the filter
   *       QueryParametersRequestFilter. No need to use @QueryParam here.
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystems(@Context SecurityContext securityContext)
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

    List<String> searchList = threadContext.getSearchList();
    if (searchList != null && !searchList.isEmpty()) _log.debug("Using searchList. First condition in list = " + searchList.get(0));

    // ------------------------- Retrieve all records -----------------------------
    List<TSystem> systems;
    try {
      systems = systemsService.getSystems(authenticatedUser, searchList, threadContext.getLimit(),
                                          threadContext.getSortBy(), threadContext.getSortByDirection(),
                                          threadContext.getSkip(), threadContext.getStartAfter());
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (systems == null) systems = Collections.emptyList();

    // ---------------------------- Success -------------------------------
    RespSystemsArray resp1 = new RespSystemsArray(systems);
//    // TODO Get total count
//    // TODO Use of metadata in response for non-dedicated search endpoints is TBD
//    RespSystems resp1 = new RespSystems(systems, threadContext.getLimit(), threadContext.getSortBy(),
//                                        threadContext.getSkip(), threadContext.getStartAfter(), -1);
    String itemCountStr = systems.size() + " systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", itemCountStr), resp1);
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

    // ------------------------- Retrieve records -----------------------------
    List<TSystem> systems;
    try {
      systems = systemsService.getSystems(authenticatedUser, searchList, threadContext.getLimit(),
                                          threadContext.getSortBy(), threadContext.getSortByDirection(),
                                          threadContext.getSkip(), threadContext.getStartAfter());
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (systems == null) systems = Collections.emptyList();

    // ------------------------- Get total count if limit/skip ignored --------------------------
    int totalCount = -1;
    if (threadContext.getComputeTotal())
    {
      // If there was no limit we already have the count, else we need to get the count
      if (threadContext.getLimit() <= 0)
      {
        totalCount = systems.size();
      }
      else
      {
        try
        {
          totalCount = systemsService.getSystemsTotalCount(authenticatedUser, threadContext.getSearchList(),
                  threadContext.getSortBy(), threadContext.getSortByDirection(),
                  threadContext.getStartAfter());
        } catch (Exception e)
        {
          String msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
          _log.error(msg, e);
          return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
      }
    }

    // ---------------------------- Success -------------------------------
    RespSystemsSearch resp1 = new RespSystemsSearch(systems, threadContext.getLimit(), threadContext.getSortBy(),
                                        threadContext.getSkip(), threadContext.getStartAfter(), totalCount);
    String itemCountStr = systems.size() + " systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", itemCountStr), resp1);
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

    // Construct final SQL-like search string using the json
    // When put together full string must be a valid SQL-like where clause. This will be validated in the service call.
    // Not all SQL syntax is supported. See SqlParser.jj in tapis-shared-searchlib.
    String searchStr;
    try
    {
      searchStr = SearchUtils.getSearchFromRequestJson(rawJson);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    _log.debug("Using search string: " + searchStr);

    // ------------------------- Retrieve records -----------------------------
    List<TSystem> systems;
    try {
      systems = systemsService.getSystemsUsingSqlSearchStr(authenticatedUser, searchStr, threadContext.getLimit(),
                                                           threadContext.getSortBy(), threadContext.getSortByDirection(),
                                                           threadContext.getSkip(), threadContext.getStartAfter());
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (systems == null) systems = Collections.emptyList();

    // ------------------------- Get total count if limit/skip ignored --------------------------
    int totalCount = -1;
    if (threadContext.getComputeTotal())
    {
      // If there was no limit we already have the count, else we need to get the count
      if (threadContext.getLimit() <= 0)
      {
        totalCount = systems.size();
      }
      else
      {
        try
        {
          totalCount = systemsService.getSystemsTotalCount(authenticatedUser, threadContext.getSearchList(),
                  threadContext.getSortBy(), threadContext.getSortByDirection(),
                  threadContext.getStartAfter());
        } catch (Exception e)
        {
          msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
          _log.error(msg, e);
          return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
      }
    }

    // ---------------------------- Success -------------------------------
    RespSystemsSearch resp1 = new RespSystemsSearch(systems, threadContext.getLimit(), threadContext.getSortBy(),
                                        threadContext.getSkip(), threadContext.getStartAfter(), totalCount);
    String itemCountStr = systems.size() + " systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", itemCountStr), resp1);
  }

  /**
   * matchConstraints
   * Retrieve details for systems. Use request body to specify constraint conditions as an SQL-like WHERE clause.
   * Request body contains an array of strings that are concatenated to form the full SQL-like search string.
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching constraint conditions.
   */
  @POST
  @Path("match/constraints")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response matchConstraints(InputStream payloadStream,
                                   @Context SecurityContext securityContext)
  {
    String opName = "matchConstraints";
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
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_MATCH_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // Construct final SQL-like search string using the json
    // When put together full string must be a valid SQL-like where clause. This will be validated in the service call.
    // Not all SQL syntax is supported. See SqlParser.jj in tapis-shared-searchlib.
    String matchStr;
    try
    {
      matchStr = SearchUtils.getMatchFromRequestJson(rawJson);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    _log.debug("Using match string: " + matchStr);

    // ------------------------- Retrieve records -----------------------------
    List<TSystem> systems;
    try {
      systems = systemsService.getSystemsSatisfyingConstraints(authenticatedUser, matchStr);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_SELECT_ERROR", authenticatedUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(RestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (systems == null) systems = Collections.emptyList();

    // ---------------------------- Success -------------------------------
    RespSystemsArray resp1 = new RespSystemsArray(systems);
    String itemCountStr = systems.size() + " systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "Systems", itemCountStr), resp1);
  }

  /**
   * deleteSystem
   * @param systemId - name of the system to delete
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @DELETE
  @Path("{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
// TODO/TBD Add query parameter "confirm" which must be set to true since this is an operation that cannot be undone by a user
  public Response deleteSystem(@PathParam("systemId") String systemId,
                               @Context SecurityContext securityContext)
  {
    String opName = "deleteSystem";
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
      changeCount = systemsService.softDeleteSystem(authenticatedUser, systemId);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_DELETE_NAME_ERROR", authenticatedUser, systemId, e.getMessage());
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
      MsgUtils.getMsg("TAPIS_DELETED", "System", systemId), prettyPrint, resp1)).build();
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Create a TSystem from a ReqCreateSystem
   */
  private static TSystem createTSystemFromRequest(ReqCreateSystem req)
  {
    var system = new TSystem(-1, null, req.id, req.description, req.systemType, req.owner, req.host,
                       req.enabled, req.effectiveUserId, req.defaultAuthnMethod, req.bucketName, req.rootDir,
                       req.transferMethods, req.port, req.useProxy, req.proxyHost, req.proxyPort,
                       req.dtnSystemId, req.dtnMountPoint, req.dtnSubDir, req.canExec, req.jobWorkingDir,
                       req.jobEnvVariables, req.jobMaxJobs, req.jobMaxJobsPerUser, req.jobIsBatch, req.batchScheduler,
                       req.batchDefaultLogicalQueue, req.tags, req.notes, req.refImportId, false, null, null);
    system.setAuthnCredential(req.authnCredential);
    system.setBatchLogicalQueues(req.batchLogicalQueues);
    system.setJobCapabilities(req.jobCapabilities);
    return system;
  }

  /**
   * Create a PatchSystem from a ReqUpdateSystem
   */
  private static PatchSystem createPatchSystemFromRequest(ReqUpdateSystem req, String tenantName, String systemId)
  {
    PatchSystem patchSystem = new PatchSystem(req.description, req.host, req.enabled, req.effectiveUserId,
                           req.defaultAuthnMethod, req.transferMethods, req.port, req.useProxy,
                           req.proxyHost, req.proxyPort, req.dtnSystemId, req.dtnMountPoint, req.dtnSubDir,
                           req.jobWorkingDir, req.jobEnvVariables, req.jobMaxJobs, req.jobMaxJobsPerUser,
                           req.jobIsBatch, req.batchScheduler, req.batchLogicalQueues, req.batchDefaultLogicalQueue,
                           req.jobCapabilities, req.tags, req.notes);
    // Update tenant name and system name
    patchSystem.setTenant(tenantName);
    patchSystem.setId(systemId);
    return patchSystem;
  }

  /**
   * Fill in defaults and check constraints on TSystem attributes
   * Check values. systemId, host, authnMethod must be set. effectiveUserId is restricted.
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
    String id = system1.getId();
    String msg;
    var errMessages = new ArrayList<String>();
    if (StringUtils.isBlank(system1.getId()))
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_MISSING_ATTR", ID_FIELD);
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
    if (system1.getDefaultAuthnMethod() == null)
    {
      msg = ApiUtils.getMsg("SYSAPI_CREATE_MISSING_ATTR", DEFAULT_AUTHN_METHOD_FIELD);
      errMessages.add(msg);
    }
    if (system1.getDefaultAuthnMethod().equals(AuthnMethod.CERT) &&
            !effectiveUserId.equals(TSystem.APIUSERID_VAR) &&
            !effectiveUserId.equals(TSystem.OWNER_VAR) &&
            !StringUtils.isBlank(owner) &&
            !effectiveUserId.equals(owner))
    {
      // For CERT authn the effectiveUserId cannot be static string other than owner
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
    if (system1.getAuthnCredential() != null && effectiveUserId.equals(TSystem.APIUSERID_VAR))
    {
      // If effectiveUserId is dynamic then providing credentials is disallowed
      msg = ApiUtils.getMsg("SYSAPI_CRED_DISALLOWED_INPUT");
      errMessages.add(msg);
    }

    // If validation failed log error message and return response
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(errMessages, authenticatedUser, id);
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
   * AuthnCredential details can contain secrets. Mask any secrets given
   * and return a string containing the final redacted Json.
   * @param rawJson Json from request
   * @return A string with any secrets masked out
   */
  private static String maskCredSecrets(String rawJson)
  {
    if (StringUtils.isBlank(rawJson)) return rawJson;
    // Get the Json object and prepare to extract info from it
    JsonObject sysObj = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);
    if (!sysObj.has(AUTHN_CREDENTIAL_FIELD)) return rawJson;
    var credObj = sysObj.getAsJsonObject(AUTHN_CREDENTIAL_FIELD);
    maskSecret(credObj, CredentialResource.PASSWORD_FIELD);
    maskSecret(credObj, CredentialResource.PRIVATE_KEY_FIELD);
    maskSecret(credObj, CredentialResource.PUBLIC_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_SECRET_FIELD);
    maskSecret(credObj, CredentialResource.CERTIFICATE_FIELD);
    sysObj.remove(AUTHN_CREDENTIAL_FIELD);
    sysObj.add(AUTHN_CREDENTIAL_FIELD, credObj);
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
    Response r1 = Response.ok(resp).build();
    return r1;
  }
}

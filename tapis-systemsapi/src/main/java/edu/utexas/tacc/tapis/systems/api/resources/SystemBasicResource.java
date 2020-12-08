package edu.utexas.tacc.tapis.systems.api.resources;

import com.google.gson.JsonSyntaxException;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemBasic;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemsBasic;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.SystemBasic;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import org.apache.commons.io.IOUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/*
 * JAX-RS REST resource for a Tapis SystemBasic (edu.utexas.tacc.tapis.systems.model.SystemBasic)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see tapis-client-java repo file systems-client/SystemsAPI.yaml
 *       and note at top of SystemsResource.java
 *
 * NOTE: The "pretty" query parameter is available for all endpoints. It is processed in
 *       QueryParametersRequestFilter.java.
 */
@Path("/v3/systems_basic")
public class SystemBasicResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemBasicResource.class);

  // Json schema resource files.
  private static final String FILE_SYSTEM_SEARCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemSearchRequest.json";

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
   * getSystemBasic
   * @param systemName - name of the system
   * @param securityContext - user identity
   * @return Response with a basic system object as the result
   */
  @GET
  @Path("{systemName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystemBasic(@PathParam("systemName") String systemName,
                                 @Context SecurityContext securityContext)
  {
    String opName = "getSystemBasic";
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the tenant name and apiUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    boolean prettyPrint = threadContext.getPrettyPrint();
    Response resp = ApiUtils.checkContext(threadContext, prettyPrint);
    if (resp != null) return resp;

    // Get AuthenticatedUser which contains jwtTenant, jwtUser, oboTenant, oboUser, etc.
    AuthenticatedUser authenticatedUser = (AuthenticatedUser) securityContext.getUserPrincipal();

    SystemBasic system;
    try
    {
      system = systemsService.getSystemBasic(authenticatedUser, systemName);
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
    RespSystemBasic resp1 = new RespSystemBasic(system);
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "SystemBasic", systemName), resp1);
  }

  /**
   * getSystemsBasic
   * Retrieve all systems accessible by requester and matching any search conditions provided.
   * NOTE: The query parameters pretty, search, limit, sortBy, skip, startAfter are all handled in the filter
   *       QueryParametersRequestFilter. No need to use @QueryParam here.
   * @param securityContext - user identity
   * @return - list of basic systems accessible by requester and matching search conditions.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystemsBasic(@Context SecurityContext securityContext)
  {
    String opName = "getSystemsBasic";
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

    // ------------------------- Retrieve records -----------------------------
    List<SystemBasic> systems;
    try {
      systems = systemsService.getSystemsBasic(authenticatedUser, threadContext.getSearchList(), threadContext.getLimit(),
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
    // TODO Use of metadata in response for non-dedicated search endpoints is TBD
    //      See SystemResource
    RespSystemsBasic resp1 = new RespSystemsBasic(systems, threadContext.getLimit(), threadContext.getSortBy(),
                                                  threadContext.getSkip(), threadContext.getStartAfter(), totalCount);
    String itemCountStr = systems.size() + " systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "SystemsBasic", itemCountStr), resp1);
  }

  /**
   * searchSystemsBasicQueryParameters
   * Dedicated search endpoint for System resource. Search conditions provided as query parameters.
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Path("search/systems")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsBasicQueryParameters(@Context SecurityContext securityContext)
  {
    String opName = "searchSystemsBasicGet";
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
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    if (searchList != null && !searchList.isEmpty()) _log.debug("Using searchList. First value = " + searchList.get(0));

    // ------------------------- Retrieve all records -----------------------------
    List<SystemBasic> systems;
    try {
      systems = systemsService.getSystemsBasic(authenticatedUser, searchList, threadContext.getLimit(),
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
    RespSystemsBasic resp1 = new RespSystemsBasic(systems, threadContext.getLimit(), threadContext.getSortBy(),
                                        threadContext.getSkip(), threadContext.getStartAfter(), totalCount);
    String itemCountStr = systems.size() + " systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "SystemsBasic", itemCountStr), resp1);
  }

  /**
   * searchSystemsBasicRequestBody
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
  public Response searchSystemsBasicRequestBody(InputStream payloadStream,
                                           @Context SecurityContext securityContext)
  {
    String opName = "searchSystemsBasicPost";
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

    // ------------------------- Retrieve all records -----------------------------
    List<SystemBasic> systems;
    try {
      systems = systemsService.getSystemsBasicUsingSqlSearchStr(authenticatedUser, searchStr, threadContext.getLimit(),
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
    RespSystemsBasic resp1 = new RespSystemsBasic(systems, threadContext.getLimit(), threadContext.getSortBy(),
                                                  threadContext.getSkip(), threadContext.getStartAfter(), totalCount);
    String itemCountStr = systems.size() + "systems";
    return createSuccessResponse(MsgUtils.getMsg("TAPIS_FOUND", "SystemsBasic", itemCountStr), resp1);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

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

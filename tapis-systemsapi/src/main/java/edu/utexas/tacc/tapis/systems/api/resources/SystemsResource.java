package edu.utexas.tacc.tapis.systems.api.resources;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeIn;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.glassfish.grizzly.http.server.Request;

import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import org.jooq.tools.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Tapis Systems general resources including healthcheck and readycheck
 *
 * (NOT currently used) Contains annotations which generate the OpenAPI specification documents.
 *  NOTE: Switching to hand-crafted openapi located in repo tapis-client-java at systems-client/SystemsAPI.yaml
 *        Could not fully automate generation of spec and annotations have some limits. E.g., how to mark a parameter
 *        in a request body as required?, how to better describe query parameters?
 */
//@OpenAPIDefinition(
//    security = {@SecurityRequirement(name = "TapisJWT")},
//    info = @Info(
//        title = "Tapis Systems API",
//        description = "The Tapis Systems API provides for management of Tapis Systems including access and transfer methods, permissions and credentials.",
//        license = @License(name = "3-Clause BSD License", url = "https://opensource.org/licenses/BSD-3-Clause"),
//        contact = @Contact(name = "CICSupport", email = "cicsupport@tacc.utexas.edu")),
//    tags = {
//        @Tag(name = "systems", description = "manage systems")
//    },
//    servers = {
////      @Server(url = "v3/systems", description = "Base URL")
//      @Server(url = "http://localhost:8080/", description = "Local test environment"),
//      @Server(url = "https://dev.develop.tapis.io/", description = "Development environment")
//    },
//    externalDocs = @ExternalDocumentation(description = "Tapis Home", url = "https://tacc-cloud.readthedocs.io/projects/agave")
//)
//@SecurityScheme(
//  name="TapisJWT",
//  description="Tapis signed JWT token authentication",
//  type= SecuritySchemeType.APIKEY,
//  in= SecuritySchemeIn.HEADER,
//  paramName="X-Tapis-Token"
//)
@Path("/v3/systems")
public class SystemsResource
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemsResource.class);

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
  private Request _request;

  // Count the number of health check requests received.
  private static final AtomicLong _healthCheckCount = new AtomicLong();

  // Count the number of health check requests received.
  private static final AtomicLong _readyCheckCount = new AtomicLong();

  // Use CallSiteToggle to limit logging for readyCheck endpoint
  private static final CallSiteToggle checkTenantsOK = new CallSiteToggle();
  private static final CallSiteToggle checkJWTOK = new CallSiteToggle();
  private static final CallSiteToggle checkDBOK = new CallSiteToggle();

  // **************** Inject Services using HK2 ****************
  @Inject
  private SystemsServiceImpl svcImpl;
  @Inject
  private ServiceJWT serviceJWT;

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Lightweight non-authenticated health check endpoint.
   * Note that no JWT is required on this call and no logging is done.
   * @return a success response if all is ok
   */
  @GET
  @Path("/healthcheck")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
//  @Operation(
//    description = "Health check. Lightweight non-authenticated basic liveness check. Returns full version.",
//    tags = "general",
//    responses = {
//      @ApiResponse(responseCode = "200", description = "Message received.",
//        content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//      @ApiResponse(responseCode = "500", description = "Server error.")
//    }
//  )
  public Response healthCheck()
  {
    // Get the current check count.
    long checkNum = _healthCheckCount.incrementAndGet();
    RespBasic resp = new RespBasic("Health check received. Count: " + checkNum);

    // Manually create a success response with git info included in version
    resp.status = ResponseWrapper.RESPONSE_STATUS.success.name();
    resp.message = MsgUtils.getMsg("TAPIS_HEALTHY", "Systems Service");
    resp.version = TapisUtils.getTapisFullVersion();
    String respStr = TapisGsonUtils.getGson().toJson(resp);
    return Response.status(Status.OK).entity(respStr).build();
  }

  /**
   * Lightweight non-authenticated ready check endpoint.
   * Note that no JWT is required on this call and CallSiteToggle is used to limit logging.
   * Based on similar method in tapis-securityapi.../SecurityResource
   *
   * For this service readiness means service can:
   *    - retrieve tenants map
   *    - get a service JWT
   *    - connect to the DB and verify and that main service table exists
   *
   * It's intended as the endpoint that monitoring applications can use to check
   * whether the application is ready to accept traffic.  In particular, kubernetes
   * can use this endpoint as part of its pod readiness check.
   *
   * Note that no JWT is required on this call.
   *
   * A good synopsis of the difference between liveness and readiness checks:
   *
   * ---------
   * The probes have different meaning with different results:
   *
   *    - failing liveness probes  -> restart pod
   *    - failing readiness probes -> do not send traffic to that pod
   *
   * See https://stackoverflow.com/questions/54744943/why-both-liveness-is-needed-with-readiness
   * ---------
   *
   * @return a success response if all is ok
   */
  @GET
  @Path("/readycheck")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
//  @Operation(
//          description = "Ready check. Lightweight non-authenticated check that service is ready to accept requests.",
//          tags = "general",
//          responses =
//                  {@ApiResponse(responseCode = "200", description = "Service ready.",
//                          content = @Content(schema = @Schema(
//                                  implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
//                          @ApiResponse(responseCode = "503", description = "Service unavailable.")}
//  )
  public Response readyCheck()
  {
    // Get the current check count.
    long checkNum = _readyCheckCount.incrementAndGet();

    // Check that we can get tenants list
    Exception readyCheckException = checkTenants();
    if (readyCheckException != null)
    {
      RespBasic r = new RespBasic("Readiness tenants check failed. Check number: " + checkNum);
      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Systems Service");
      // We failed so set the log limiter check.
      // TODO/TBD Do we need to also check exception type? Info would get lost if exception type changes.
      if (checkTenantsOK.toggleOff())
      {
        _log.warn(msg, readyCheckException);
        _log.warn(ApiUtils.getMsg("SYSAPI_READYCHECK_TENANTS_ERRTOGGLE_SET"));
      }
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
    }
    else
    {
      // We succeeded so clear the log limiter check.
      if (checkTenantsOK.toggleOn()) _log.info(ApiUtils.getMsg("SYSAPI_READYCHECK_TENANTS_ERRTOGGLE_CLEARED"));
    }

    // Check that we have a service JWT
    readyCheckException = checkJWT();
    if (readyCheckException != null)
    {
      RespBasic r = new RespBasic("Readiness JWT check failed. Check number: " + checkNum);
      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Systems Service");
      // We failed so set the log limiter check.
      // TODO/TBD Do we need to also check exception type? Info would get lost if exception type changes.
      if (checkJWTOK.toggleOff())
      {
        _log.warn(msg, readyCheckException);
        _log.warn(ApiUtils.getMsg("SYSAPI_READYCHECK_JWT_ERRTOGGLE_SET"));
      }
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
    }
    else
    {
      // We succeeded so clear the log limiter check.
      if (checkJWTOK.toggleOn()) _log.info(ApiUtils.getMsg("SYSAPI_READYCHECK_JWT_ERRTOGGLE_CLEARED"));
    }

    // Check that we can connect to the DB
    readyCheckException = checkDB();
    if (readyCheckException != null)
    {
      RespBasic r = new RespBasic("Readiness DB check failed. Check number: " + checkNum);
      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Systems Service");
      // We failed so set the log limiter check.
      // TODO/TBD Do we need to also check exception type? Info would get lost if exception type changes.
      if (checkDBOK.toggleOff())
      {
        _log.warn(msg, readyCheckException);
        _log.warn(ApiUtils.getMsg("SYSAPI_READYCHECK_DB_ERRTOGGLE_SET"));
      }
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
    }
    else
    {
      // We succeeded so clear the log limiter check.
      if (checkDBOK.toggleOn()) _log.info(ApiUtils.getMsg("SYSAPI_READYCHECK_DB_ERRTOGGLE_CLEARED"));
    }

    // ---------------------------- Success -------------------------------
    // Create the response payload.
    RespBasic resp = new RespBasic("Ready check passed. Count: " + checkNum);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            MsgUtils.getMsg("TAPIS_READY", "Systems Service"), false, resp)).build();
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Verify that we have a valid service JWT.
   * @return null if OK, otherwise return an exception
   */
  private Exception checkJWT()
  {
    Exception result = null;
    try {
      String jwt = serviceJWT.getAccessJWT();
      if (StringUtils.isBlank(jwt)) result = new TapisClientException(LibUtils.getMsg("SYSLIB_CHECKJWT_EMPTY"));
    }
    catch (Exception e) { result = e; }
    return result;
  }

  /**
   * Check the database
   * @return null if OK, otherwise return an exception
   */
  private Exception checkDB()
  {
    Exception result = null;
    try { result = svcImpl.checkDB(); }
    catch (Exception e) { result = e; }
    return result;
  }

  /**
   * Retrieve the cached tenants map.
   * @return null if OK, otherwise return an exception
   */
  private Exception checkTenants()
  {
    Exception result = null;
    try
    {
      // Make sure the cached tenants map is not null or empty.
      var tenantMap = TenantManager.getInstance().getTenants();
      if (tenantMap == null || tenantMap.isEmpty()) result = new TapisClientException(LibUtils.getMsg("SYSLIB_CHECKTENANTS_EMPTY"));
    }
    catch (Exception e) { result = e; }
    return result;
  }
}

package edu.utexas.tacc.tapis.security.api.resources;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.security.PermitAll;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.secrets.VaultManager;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
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

@OpenAPIDefinition(
        security = {@SecurityRequirement(name = "TapisJWT")},
        info = @Info(title = "Tapis Security API",
                     version = "0.1",
                     description = "The Tapis Security API provides access to the " +
                     "Tapis Security Kernel authorization and secrets facilities.",
                     license = @License(name = "3-Clause BSD License", url = "https://opensource.org/licenses/BSD-3-Clause"),
                     contact = @Contact(name = "CICSupport", 
                                        email = "cicsupport@tacc.utexas.edu")),
        tags = {@Tag(name = "role", description = "manage roles and permissions"),
                @Tag(name = "user", description = "assign roles and permissions to users"),
                @Tag(name = "vault", description = "manage application and user secrets"),
                @Tag(name = "general", description = "informational endpoints")},
        servers = {@Server(url = "http://localhost:8080/v3", description = "Local test environment")},
        externalDocs = @ExternalDocumentation(description = "Tapis Home",
                                     url = "https://tacc-cloud.readthedocs.io/projects/agave/en/latest/")
)
@SecurityScheme(
        name="TapisJWT",
        description="Tapis signed JWT token authentication",
        type=SecuritySchemeType.APIKEY,
        in=SecuritySchemeIn.HEADER,
        paramName="X-Tapis-Token"
)
@Path("/")
public final class SecurityResource
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SecurityResource.class);
    
    // Database check timeouts.
    private static final long DB_READY_TIMEOUT_MS  = 6000;   // 6 seconds.
    private static final long DB_HEALTH_TIMEOUT_MS = 60000;  // 1 minute.
    
    // The table we query during readiness checks.
    private static final String QUERY_TABLE = "sk_role";
    
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
     private HttpHeaders        _httpHeaders;
  
     @Context
     private Application        _application;
  
     @Context
     private UriInfo            _uriInfo;
  
     @Context
     private SecurityContext    _securityContext;
  
     @Context
     private ServletContext     _servletContext;
  
     @Context
     private HttpServletRequest _request;
     
     // Count the number of healthchecks requests received.
     private static final AtomicLong _healthChecks = new AtomicLong();
    
     // Count the number of healthchecks requests received.
     private static final AtomicLong _readyChecks = new AtomicLong();
     
     // The flag set after the first successful database readiness check.
     private static final AtomicBoolean _readyDBOnce = new AtomicBoolean();
     
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* hello:                                                                       */
  /* ---------------------------------------------------------------------------- */
  @GET
  @Path("/hello")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
          description = "Logged connectivity test.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Message received.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
               @ApiResponse(responseCode = "401", description = "Not authorized."),
               @ApiResponse(responseCode = "500", description = "Server error.")}
      )
  public Response sayHello(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
  {
      // Trace this request.
      if (_log.isTraceEnabled()) {
          String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hello", 
                                     "  " + _request.getRequestURL());
          _log.trace(msg);
      }
      
      // Create the response payload.
      RespBasic r = new RespBasic("Hello from the Tapis Security Kernel.");
         
      // ---------------------------- Success ------------------------------- 
      // Success means we found the resource. 
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_FOUND", "hello", "0 items"), prettyPrint, r)).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* healthcheck:                                                                 */
  /* ---------------------------------------------------------------------------- */
  /** This method does no logging and is expected to be as lightwieght as possible.
   * It's intended as the endpoint that monitoring applications can use to check
   * the liveness (i.e, no deadlocks) of the application.  In particular, 
   * kubernetes can use this endpoint as part of its pod health check.
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
  @Path("/healthcheck")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @Operation(
          description = "Lightwieght health check for liveness.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Message received.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
               @ApiResponse(responseCode = "503", description = "Service unavailable.")}
      )
  public Response checkHealth()
  {
      // Get the current check count.
      long checkNum = _healthChecks.incrementAndGet();
      
      // Check the database only after a DB readiness check has succeeded.
      if (_readyDBOnce.get()) 
          if (!queryDB(DB_HEALTH_TIMEOUT_MS)) {
              // Failure case.
              RespBasic r = new RespBasic("Health DB check " + checkNum + " failed.");
              String msg = MsgUtils.getMsg("TAPIS_NOT_HEALTHY", "Security Kernel");
              return Response.status(Status.SERVICE_UNAVAILABLE).
                  entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
          }
      
      // Check the tenant manager.
      if (!queryTenants()) {
          // Failure case.
          RespBasic r = new RespBasic("Readiness tenants check " + checkNum + " failed.");
          String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Security Kernel");
          return Response.status(Status.SERVICE_UNAVAILABLE).
              entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
      }
      
      // Check the health of vault.
      var vaultMgr = VaultManager.getInstance(true);
      if (vaultMgr == null || !vaultMgr.isHealthy()) {
          // Failure case.
          RespBasic r = new RespBasic("Health secrets check " + checkNum + " failed.");
          String msg = MsgUtils.getMsg("TAPIS_NOT_HEALTHY", "Security Kernel");
          return Response.status(Status.SERVICE_UNAVAILABLE).
              entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
      }
      
      // ---------------------------- Success ------------------------------- 
      // Create the response payload.
      RespBasic r = new RespBasic("Healthcheck " + checkNum + " received.");
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_HEALTHY", "Security Kernel"), false, r)).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* ready:                                                                       */
  /* ---------------------------------------------------------------------------- */
  /** This method does no logging and is expected to be as lightwieght as possible.
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
  @Path("/ready")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @Operation(
          description = "Lightwieght readiness check.",
          tags = "general",
          responses = 
              {@ApiResponse(responseCode = "200", description = "Service ready.",
                   content = @Content(schema = @Schema(
                       implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
               @ApiResponse(responseCode = "503", description = "Service unavailable.")}
      )
  public Response ready()
  {
      // Get the current check count.
      long checkNum = _readyChecks.incrementAndGet();
      
      // Test connectivity only if no success has ever been recorded.
      // There could be a race condition here but the worst that could
      // happen is an extra readiness check or two would query the db.
      if (!_readyDBOnce.get()) 
          if (queryDB(DB_READY_TIMEOUT_MS)) _readyDBOnce.set(true);
            else {
                // Failure case.
                RespBasic r = new RespBasic("Readiness DB check " + checkNum + " failed.");
                String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Security Kernel");
                return Response.status(Status.SERVICE_UNAVAILABLE).
                    entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
            }
      
      // Check the tenant manager.
      if (!queryTenants()) {
          // Failure case.
          RespBasic r = new RespBasic("Readiness tenants check " + checkNum + " failed.");
          String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Security Kernel");
          return Response.status(Status.SERVICE_UNAVAILABLE).
              entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
      }
      
      // Check the readiness of vault.
      var vaultMgr = VaultManager.getInstance(true);
      if (vaultMgr == null || !vaultMgr.isReady()) {
          // Failure case.
          RespBasic r = new RespBasic("Readiness secrets check " + checkNum + " failed.");
          String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Security Kernel");
          return Response.status(Status.SERVICE_UNAVAILABLE).
              entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
      }
      
      // ---------------------------- Success -------------------------------
      // Create the response payload.
      RespBasic r = new RespBasic("Readiness check " + checkNum + " received.");
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_READY", "Security Kernel"), false, r)).build();
  }

  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* queryDB:                                                                     */
  /* ---------------------------------------------------------------------------- */
  /** Probe the database with a simple database query.
   * 
   * @param timeoutMillis millisecond limit for success
   * @return true for success, false otherwise
   */
  private boolean queryDB(long timeoutMillis)
  {
      // Start optimistically.
      boolean success = true;
      
      // Any db error or a time expiration fails the connectivity check.
      try {
          // Try to run a simple query.
          long startTime = Instant.now().toEpochMilli();
          int result = getRoleImpl().queryDB(QUERY_TABLE);
          
          // Did the query take too long?
          long elapsed = Instant.now().toEpochMilli() - startTime;
          if (elapsed > timeoutMillis) {
              String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Security Kernel", 
                                           "Excessive query time (" + elapsed + " milliseconds)");
              _log.error(msg);
              success = false;
          }
      }
      catch (Exception e) {
          // Any exception causes us to report failure.
          String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Security Kernel", e.getMessage());
          _log.error(msg, e);
          success = false;
      }
      
      return success;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* queryTenants:                                                                */
  /* ---------------------------------------------------------------------------- */
  /** Retrieve the cached tenants map.
   * 
   * @return true if the map is not null, false otherwise
   */
  private boolean queryTenants()
  {
      // Start optimistically.
      boolean success = true;
      
      try {
          // Make sure the cached tenants map is not null.
          var tenantMap = TenantManager.getInstance().getTenants();
          if (tenantMap == null) {
              String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Security Kernel", 
                                           "Null tenants map.");
              _log.error(msg);
              success = false;
          }
      } catch (Exception e) {
          String msg = MsgUtils.getMsg("TAPIS_PROBE_ERROR", "Security Kernel", 
                                       e.getMessage());
          _log.error(msg, e);
          success = false;
      }
      
      return success;
  }
  
}

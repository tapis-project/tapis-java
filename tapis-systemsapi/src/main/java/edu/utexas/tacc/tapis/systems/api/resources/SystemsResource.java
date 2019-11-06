package edu.utexas.tacc.tapis.systems.api.resources;

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

import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

@OpenAPIDefinition(
    security = {@SecurityRequirement(name = "Tapis JWT")},
    info = @Info(
        title = "Tapis Systems API",
        version = "0.1",
        description = "The Tapis Systems API provides for management of Tapis Systems including access and transfer protocols and credentials.",
        license = @License(name = "3-Clause BSD License", url = "https://opensource.org/licenses/BSD-3-Clause"),
        contact = @Contact(name = "CICSupport", email = "cicsupport@tacc.utexas.edu")),
    tags = {
        @Tag(name = "systems", description = "manage systems")
    },
    servers = {@Server(url = "http://localhost:8080", description = "Local test environment")},
    externalDocs = @ExternalDocumentation(description = "Tapis Home",
        url = "https://tacc-cloud.readthedocs.io/projects/agave")
)
@Path("/")
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
  private HttpServletRequest _request;

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  @GET
  @Path("/hello")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      description = "Connectivity test.",
      tags = "general",
      responses = {
          @ApiResponse(responseCode = "200", description = "Message received."),
          @ApiResponse(responseCode = "401", description = "Not authorized."),
          @ApiResponse(responseCode = "500", description = "Server error.")
      }
  )
  public Response getHello()
  {
    // Trace this request.
    if (_log.isTraceEnabled())
    {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hello",
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }

    // ---------------------------- Success -------------------------------
    // Success means we are alive
    ResultName result = new ResultName();
    result.name = "Hello from Tapis Systems Service";
    RespName resp = new RespName(result);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
      MsgUtils.getMsg("TAPIS_FOUND", "hello", "no items"), true, resp)).build();
  }

  /**
   * Lightweight non-authenticated health check endpoint.
   *
   * @return a sucesss response if all is ok
   */
  @GET
  @Path("/healthcheck")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
    description = "Health check.",
    tags = "general",
    responses = {
      @ApiResponse(responseCode = "200", description = "Message received."),
      @ApiResponse(responseCode = "500", description = "Server error.")
    }
  )
  public Response healthCheck()
  {
    RespBasic resp = new RespBasic("Healthcheck");
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
      MsgUtils.getMsg("TAPIS_HEALTHY", "Systems Service"), false, resp)).build();
  }
}

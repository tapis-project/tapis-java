package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;

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

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.requestBody.CreateRole;
import edu.utexas.tacc.tapis.security.api.responseBody.Names;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/role")
public class RoleResource 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(RoleResource.class);
    
    // Json schema resource files.
    private static final String FILE_SK_CREATE_ROLE_REQUEST = 
        "/edu/utexas/tacc/tapis/security/api/jsonschema/CreateRoleRequest.json";

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
    
     /* **************************************************************************** */
     /*                                Public Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* getRoleNames:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the names of all roles in the tenant.",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of role names returned.",
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.Names.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getRoleNames(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getRoleNames", "  " + _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         Names names = new Names();
         names.names = new String[2];
         names.names[0] = "xxx";
         names.names[1] = "yyy";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the job. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "roles"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getRoleByName:                                                               */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Get the named role's definition.",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Named role returned.",
               content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.authz.model.SkRole.class))),
              @ApiResponse(responseCode = "400", description = "Input error."),
              @ApiResponse(responseCode = "401", description = "Not authorized."),
              @ApiResponse(responseCode = "404", description = "Named role not found."),
              @ApiResponse(responseCode = "500", description = "Server error.")}
     )
     public Response getRoleByName(@PathParam("roleName") String roleName,
                                   @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getRoleByName", 
                                          "  " + _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         SkRole role = new SkRole();
         role.setId(88);
         role.setName(roleName);
         role.setTenant("faketenant");
         role.setDescription("blah, blah, blah");
         role.setCreatedby("bozo");
         role.setUpdatedby("bozo");
         role.setCreated(Instant.now());
         role.setUpdated(Instant.now());
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the job. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "role"), prettyPrint, role)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* createRole:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Create a role using either a request body or query parameters, but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.requestBody.CreateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role created."),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response createRole(@QueryParam("roleName") String roleName,
                                @QueryParam("description") String description,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "createRole", 
                                          "  " + _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Code
         // Either query parameters are used or payload, but not a mixture.
         if (roleName == null || description == null) {
             // There better be a payload.
             String json = null;
             try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
               catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job submission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }
             
             // Create validator specification.
             JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SK_CREATE_ROLE_REQUEST);
             
             // Make sure the json conforms to the expected schema.
             try {JsonValidator.validate(spec);}
               catch (TapisJSONException e) {
                 String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }

             CreateRole createRolePayload = null;
             try {createRolePayload = TapisGsonUtils.getGson().fromJson(json, CreateRole.class);}
                 catch (Exception e) {
                     String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());            
                     _log.error(msg, e);
                     return Response.status(Status.BAD_REQUEST).
                             entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
                 }
             
             // Fill in the parameter fields.
             roleName = createRolePayload.roleName;
             description = createRolePayload.description;
         }
         
         System.out.println("***** roleName    = " + roleName);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the job. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_CREATED", "Role"), prettyPrint, "Create role")).build();
     }
}

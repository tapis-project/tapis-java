package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
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
import edu.utexas.tacc.tapis.security.api.requestBody.UpdateRole;
import edu.utexas.tacc.tapis.security.api.responseBody.ChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.NameArray;
import edu.utexas.tacc.tapis.security.api.responseBody.ResourceUrl;
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
    private static final String FILE_SK_UPDATE_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UpdateRoleRequest.json";

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
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.NameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getRoleNames(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getRoleNames", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         NameArray names = new NameArray();
         names.names = new String[2];
         names.names[0] = "xxx";
         names.names[1] = "yyy";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = (names == null || names.names == null) ? 0 : names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Roles", cnt + " items"), prettyPrint, names)).build();
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
               content = @Content(schema = @Schema(
                   implementation = edu.utexas.tacc.tapis.security.authz.model.SkRole.class))),
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
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getRoleByName", _request.getRequestURL());
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
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Role", roleName), prettyPrint, role)).build();
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
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.CreateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "201", description = "Role created.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.ResourceUrl.class))),
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
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "createRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Either query parameters are used or payload, but not a mixture.
         if (roleName == null || description == null) {
             // There better be a payload.
             String json = null;
             try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
               catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "create role", e.getMessage());
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
         
         // ***** DUMMY TEST Code
         System.out.println("***** roleName    = " + roleName);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         // NOTE: We need to assign a location header as well.
         //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
         ResourceUrl requestUrl = new ResourceUrl();
         requestUrl.url = _request.getRequestURL().toString() + "/" + roleName;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we created the role. 
         return Response.status(Status.CREATED).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_CREATED", "Role", roleName), prettyPrint, requestUrl)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* deleteRoleByName:                                                            */
     /* ---------------------------------------------------------------------------- */
     @DELETE
     @Path("/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Delete named role.",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Role deleted.",
                 content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.security.api.responseBody.ChangeCount.class))),
              @ApiResponse(responseCode = "400", description = "Input error."),
              @ApiResponse(responseCode = "401", description = "Not authorized."),
              @ApiResponse(responseCode = "500", description = "Server error.")}
     )
     public Response deleteRoleByName(@PathParam("roleName") String roleName,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "deleteRoleByName", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         ChangeCount count = new ChangeCount();
         count.changes = 1;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we deleted the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_DELETED", "Role", roleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* updateRole:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Update an existing role using either a request body or query parameters, "
                           + "but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.UpdateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role updated.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.ChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response updateRole(@QueryParam("roleName") String roleName,
                                @QueryParam("description") String description,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "updateRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Either query parameters are used or payload, but not a mixture.
         if (roleName == null && description == null) {
             // There better be a payload.
             String json = null;
             try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
               catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "create role", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }
             
             // Create validator specification.
             JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SK_UPDATE_ROLE_REQUEST);
             
             // Make sure the json conforms to the expected schema.
             try {JsonValidator.validate(spec);}
               catch (TapisJSONException e) {
                 String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }

             UpdateRole updateRolePayload = null;
             try {updateRolePayload = TapisGsonUtils.getGson().fromJson(json, UpdateRole.class);}
                 catch (Exception e) {
                     String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());            
                     _log.error(msg, e);
                     return Response.status(Status.BAD_REQUEST).
                             entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
                 }
             
             // Fill in the parameter fields.
             roleName = updateRolePayload.roleName;
             description = updateRolePayload.description;
         }
         
         // ***** DUMMY TEST Code
         System.out.println("***** roleName    = " + roleName);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         ChangeCount count = new ChangeCount();
         count.changes = 2;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint, count)).build();
     }
}

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

import edu.utexas.tacc.tapis.security.api.requestBody.CreatePermission;
import edu.utexas.tacc.tapis.security.api.requestBody.UpdatePermission;
import edu.utexas.tacc.tapis.security.api.responseBody.ChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.NameArray;
import edu.utexas.tacc.tapis.security.api.responseBody.ResourceUrl;
import edu.utexas.tacc.tapis.security.authz.model.SkPermission;
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

@Path("/perm")
public class PermissionResource 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(PermissionResource.class);
    
    // Json schema resource files.
    private static final String FILE_SK_CREATE_PERM_REQUEST = 
        "/edu/utexas/tacc/tapis/security/api/jsonschema/CreatePermissionRequest.json";
    private static final String FILE_SK_UPDATE_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UpdatePermissionRequest.json";

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
     /* getPermissionNames:                                                          */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the names of all permissions in the tenant.",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of permission names returned.",
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.NameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getPermissionNames(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getPermissionNames", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         NameArray names = new NameArray();
         names.names = new String[2];
         names.names[0] = "aaa";
         names.names[1] = "bbb";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's permission names.
         int cnt = (names == null || names.names == null) ? 0 : names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Permissions", cnt + " items"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getPermissionByName:                                                         */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/{permName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Get the named permission's definition.",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Named permission returned.",
               content = @Content(schema = @Schema(
                   implementation = edu.utexas.tacc.tapis.security.authz.model.SkPermission.class))),
              @ApiResponse(responseCode = "400", description = "Input error."),
              @ApiResponse(responseCode = "401", description = "Not authorized."),
              @ApiResponse(responseCode = "404", description = "Named permission not found."),
              @ApiResponse(responseCode = "500", description = "Server error.")}
     )
     public Response getPermissionByName(@PathParam("permName") String permName,
                                         @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getPermssionByName", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         SkPermission perm = new SkPermission();
         perm.setId(66);
         perm.setName(permName);
         perm.setPerm("fake:*:write");
         perm.setTenant("faketenant");
         perm.setDescription("blah, blah, blah");
         perm.setCreatedby("bozo");
         perm.setUpdatedby("bozo");
         perm.setCreated(Instant.now());
         perm.setUpdated(Instant.now());
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the permission. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Permission", permName), prettyPrint, perm)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* createPermission:                                                            */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Create a permission using either a request body or query parameters, "
                           + "but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.CreatePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "201", description = "Permission created.",
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.ResourceUrl.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response createPermission(@QueryParam("permName") String permName,
                                      @QueryParam("permValue") String permValue,
                                      @QueryParam("description") String description,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                      InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "createPermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Either query parameters are used or payload, but not a mixture.
         if (permName == null || permValue == null || description == null) {
             // There better be a payload.
             String json = null;
             try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
               catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "create permission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }
             
             // Create validator specification.
             JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SK_CREATE_PERM_REQUEST);
             
             // Make sure the json conforms to the expected schema.
             try {JsonValidator.validate(spec);}
               catch (TapisJSONException e) {
                 String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }

             CreatePermission createPermissionPayload = null;
             try {createPermissionPayload = TapisGsonUtils.getGson().fromJson(json, CreatePermission.class);}
                 catch (Exception e) {
                     String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());            
                     _log.error(msg, e);
                     return Response.status(Status.BAD_REQUEST).
                             entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
                 }
             
             // Fill in the parameter fields.
             permName = createPermissionPayload.permName;
             permValue = createPermissionPayload.permValue;
             description = createPermissionPayload.description;
         }
         
         // ***** DUMMY TEST Code
         System.out.println("***** permName    = " + permName);
         System.out.println("***** permValue   = " + permValue);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         // NOTE: We need to assign a location header as well.
         //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
         ResourceUrl requestUrl = new ResourceUrl();
         requestUrl.url = _request.getRequestURL().toString() + "/" + permName;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we created the permission. 
         return Response.status(Status.CREATED).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_CREATED", "Permission", permName), prettyPrint, requestUrl)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* deletePermissionByName:                                                      */
     /* ---------------------------------------------------------------------------- */
     @DELETE
     @Path("/{permName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Delete named permission.",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Permission deleted.",
                 content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.security.api.responseBody.ChangeCount.class))),
              @ApiResponse(responseCode = "400", description = "Input error."),
              @ApiResponse(responseCode = "401", description = "Not authorized."),
              @ApiResponse(responseCode = "500", description = "Server error.")}
     )
     public Response deletePermissionByName(@PathParam("permName") String permName,
                                            @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "deletePermissionByName", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ***** DUMMY TEST Response Data
         ChangeCount count = new ChangeCount();
         count.changes = 1;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we deleted the permission. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_DELETED", "Permission", permName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* updatePermission:                                                            */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{permName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Update an existing permission using either a request body or query parameters, "
                           + "but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.UpdatePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission updated.",
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.ChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response updatePermission(@QueryParam("permName") String permName,
                                      @QueryParam("permValue") String permValue,
                                      @QueryParam("description") String description,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                      InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "UpdatePermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Either query parameters are used or payload, but not a mixture.
         if (permName == null && permValue == null && description == null) {
             // There better be a payload.
             String json = null;
             try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
               catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "update permission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }
             
             // Create validator specification.
             JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SK_UPDATE_PERM_REQUEST);
             
             // Make sure the json conforms to the expected schema.
             try {JsonValidator.validate(spec);}
               catch (TapisJSONException e) {
                 String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                         entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
               }

             UpdatePermission updatePermPayload = null;
             try {updatePermPayload = TapisGsonUtils.getGson().fromJson(json, UpdatePermission.class);}
                 catch (Exception e) {
                     String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());            
                     _log.error(msg, e);
                     return Response.status(Status.BAD_REQUEST).
                             entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
                 }
             
             // Fill in the parameter fields.
             permName = updatePermPayload.permName;
             permValue = updatePermPayload.permValue;
             description = updatePermPayload.description;
         }
         
         // ***** DUMMY TEST Code
         System.out.println("***** permName    = " + permName);
         System.out.println("***** permValue   = " + permValue);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         ChangeCount count = new ChangeCount();
         count.changes = 3;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the permission. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Permission", permName), prettyPrint, count)).build();
     }
}

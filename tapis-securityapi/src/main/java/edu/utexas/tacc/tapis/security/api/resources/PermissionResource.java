package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.requestBody.ReqCreatePermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdatePermission;
import edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray;
import edu.utexas.tacc.tapis.security.api.responseBody.RespResourceUrl;
import edu.utexas.tacc.tapis.security.authz.model.SkPermission;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/perm")
public final class PermissionResource 
 extends AbstractResource
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
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
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
         RespNameArray names = new RespNameArray();
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
                           + "but not both.  Permission names are case sensitive, alpha-numeric "
                           + "strings that can also contain underscores.  Permission names must "
                           + "start with an alphbetic character and can be no more than 60 "
                           + "characters in length.  The desciption can be no more than "
                           + "2048 characters long.  The permission value is a colon (:) separated "
                           + "tuple in which the asterisk (*) and comma (,) are reserved characters.  "
                           + "Permission values have a limit of 1024 characters.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqCreatePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "201", description = "Permission created.",
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespResourceUrl.class))),
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
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(permName, permValue, description)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "permName, permValue, description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (permName == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqCreatePermission payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_CREATE_PERM_REQUEST, 
                                       ReqCreatePermission.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "createPermission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             permName = payload.permName;
             permValue = payload.permValue;
             description = payload.description;
         }
         
         // Final checks.
         if (StringUtils.isBlank(permName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createPermission", "permName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permValue)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createPermission", "permValue");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(description)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createPermission", "description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** permName    = " + permName);
         System.out.println("***** permValue   = " + permValue);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         // NOTE: We need to assign a location header as well.
         //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
         RespResourceUrl requestUrl = new RespResourceUrl();
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
                     implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
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
         RespChangeCount count = new RespChangeCount();
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
                           + "but not both.  Permission names are case sensitive, alpha-numeric "
                           + "strings that can also contain underscores.  Permission names must "
                           + "start with an alphbetic character and can be no more than 60 "
                           + "characters in length.  The desciption can be no more than "
                           + "2048 characters long.  The permission value is a colon (:) separated "
                           + "tuple in which the asterisk (*) and comma (,) are reserved characters.  "
                           + "Permission values have a limit of 1024 characters.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdatePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission updated.",
                     content = @Content(schema = @Schema(implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
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
         
         // ------------------------- Input Processing -------------------------
         // If all query parameters are null, we need to use the payload.
         if (allNull(permName, permValue, description)) {
             // Parse and validate the json in the request payload, which must exist.
             ReqUpdatePermission payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_UPDATE_PERM_REQUEST, 
                                       ReqUpdatePermission.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "updatePermission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             permName = payload.permName;
             permValue = payload.permValue;
             description = payload.description;
         }
         
         // Final checks.
         if (StringUtils.isBlank(permName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updatePermission", "permName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permValue)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updatePermission", "permValue");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(description)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updatePermission", "description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** permName    = " + permName);
         System.out.println("***** permValue   = " + permValue);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 3;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the permission. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Permission", permName), prettyPrint, count)).build();
     }
}

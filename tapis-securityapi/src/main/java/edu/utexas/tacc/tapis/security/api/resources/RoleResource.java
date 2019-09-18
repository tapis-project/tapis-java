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

import edu.utexas.tacc.tapis.security.api.requestBody.ReqAddChildRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqAddRolePermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqCreateRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveRolePermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRole;
import edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray;
import edu.utexas.tacc.tapis.security.api.responseBody.RespResourceUrl;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/role")
public final class RoleResource 
 extends AbstractResource
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
    private static final String FILE_SK_ADD_ROLE_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/AddRolePermissionRequest.json";
    private static final String FILE_SK_ADD_CHILD_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/AddChildRoleRequest.json";
    private static final String FILE_SK_REMOVE_ROLE_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RemoveRolePermissionRequest.json";

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
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
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
         RespNameArray names = new RespNameArray();
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
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqCreateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "201", description = "Role created.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespResourceUrl.class))),
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
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(roleName, description)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "roleName, description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (roleName == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqCreateRole payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_CREATE_ROLE_REQUEST, 
                                       ReqCreateRole.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "createRole", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             roleName = payload.roleName;
             description = payload.description;
         }
         
         // Final checks.
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(description)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         
         // ***** DUMMY TEST Code
         System.out.println("***** roleName    = " + roleName);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         // NOTE: We need to assign a location header as well.
         //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
         RespResourceUrl requestUrl = new RespResourceUrl();
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
                     implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
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
         RespChangeCount count = new RespChangeCount();
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
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role updated.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
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
         
         // ------------------------- Input Processing -------------------------
         // If all query parameters are null, we need to use the payload.
         if (allNull(roleName, description)) {
             // Parse and validate the json in the request payload, which must exist.
             ReqUpdateRole payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_UPDATE_ROLE_REQUEST, 
                                       ReqUpdateRole.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "updateRole", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             roleName = payload.roleName;
             description = payload.description;
         }
         
         // By this point there should be at least one non-null parameter.
         if (allNull(roleName, description)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "roleName, description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Final checks.
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRole", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(description)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRole", "description");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** roleName    = " + roleName);
         System.out.println("***** description = " + description);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 2;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* addRolePermission:                                                           */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/addPerm")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Add a permission to an existing role using either a request body "
                         + "or query parameters, but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqAddRolePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission assigned to role.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named resource not found."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response addRolePermission(@QueryParam("roleName") String roleName,
                                       @QueryParam("permName") String permName,
                                       @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                       InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "addRolePermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(roleName, permName)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "roleName, permName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (roleName == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqAddRolePermission payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_ADD_ROLE_PERM_REQUEST, 
                                       ReqAddRolePermission.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "addRolePermission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             roleName = payload.roleName;
             permName = payload.permName;
         }
         
         // Final checks.
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "permName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** roleName = " + roleName);
         System.out.println("***** permName = " + permName);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 2;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* removeRolePermission:                                                        */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/removePerm")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Remove a permission from a role using either a request body "
                         + "or query parameters, but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveRolePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission removed from role.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named resource not found."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response removeRolePermission(@QueryParam("roleName") String roleName,
                                          @QueryParam("permName") String permName,
                                          @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                          InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "removeRolePermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(roleName, permName)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "roleName, permName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (roleName == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqRemoveRolePermission payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_REMOVE_ROLE_PERM_REQUEST, 
                                       ReqRemoveRolePermission.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "removeRolePermission", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             roleName = payload.roleName;
             permName = payload.permName;
         }
         
         // Final checks.
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "permName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** roleName = " + roleName);
         System.out.println("***** permName = " + permName);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 1;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* addChildRole:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/addChild")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Add a child role to another role using either a request body "
                         + "or query parameters, but not both.",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqAddChildRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Child assigned to parent role.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named resource not found."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response addChildRole(@QueryParam("parentRoleName") String parentRoleName,
                                  @QueryParam("childRoleName") String childRoleName,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                  InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "addChildRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(parentRoleName, childRoleName)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "parentRoleName, childRoleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (parentRoleName == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqAddChildRole payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_ADD_CHILD_ROLE_REQUEST, 
                                       ReqAddChildRole.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "addChildRole", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             parentRoleName = payload.parentRoleName;
             childRoleName = payload.childRoleName;
         }
         
         // Final checks.
         if (StringUtils.isBlank(parentRoleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addChildRole", "parentRoleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(childRoleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addChildRole", "childRoleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** parentRoleName = " + parentRoleName);
         System.out.println("***** childRoleName = " + childRoleName);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 2;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", parentRoleName), prettyPrint, count)).build();
     }
}

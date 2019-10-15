package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.List;

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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRoleWithPermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveUserRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermitted;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti;
import edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized;
import edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl.AuthOperation;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/user")
public final class UserResource
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(UserResource.class);
    
    // Json schema resource files.
    private static final String FILE_SK_GRANT_USER_ROLE_REQUEST = 
        "/edu/utexas/tacc/tapis/security/api/jsonschema/GrantUserRoleRequest.json";
    private static final String FILE_SK_GRANT_USER_ROLE_WITH_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/GrantUserRoleWithPermRequest.json";
    private static final String FILE_SK_REMOVE_USER_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RemoveUserRoleRequest.json";
    private static final String FILE_SK_USER_HAS_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UserHasRoleRequest.json";
    private static final String FILE_SK_USER_IS_PERMITTED_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UserIsPermittedRequest.json";
    private static final String FILE_SK_USER_HAS_ROLE_MULTI_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UserHasRoleMultiRequest.json";
    private static final String FILE_SK_USER_IS_PERMITTED_MULTI_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UserIsPermittedMultiRequest.json";
    
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
     /* getUserNames:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the names of all users in the tenant that "
                           + "have been granted a role or permission.",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Sorted list of user names.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getUserNames(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUserNames", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         List<String> users = null;
         try {users = getUserImpl().getUserNames(threadContext.getTenantId());}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Populate response.
         RespNameArray names = new RespNameArray();
         String[] array = new String[users.size()];
         names.names = users.toArray(array);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " users"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUserRoles:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/roles/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the roles assigned to a user, including those assigned transively.",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of roles names assigned to the user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getUserRoles(@PathParam("user") String user,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUserRoles", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         List<String> roles = null;
         try {roles = getUserImpl().getUserRoles(threadContext.getTenantId(), user);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Populate response.
         RespNameArray names = new RespNameArray();
         String[] array = new String[roles.size()];
         names.names = roles.toArray(array);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Roles", cnt + " roles"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUserPerms:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/perms/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the permissions assigned to a user, including those assigned transively.",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of permissions assigned to the user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getUserPerms(@PathParam("user") String user,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUserPerms", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         List<String> perms = null;
         try {perms = getUserImpl().getUserPerms(threadContext.getTenantId(), user);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Populate response.
         RespNameArray names = new RespNameArray();
         String[] array = new String[perms.size()];
         names.names = perms.toArray(array);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Permissions", cnt + " permissions"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantRole:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantRole")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified role.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role assigned to user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response grantRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                               InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "grantRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqGrantUserRole payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_GRANT_USER_ROLE_REQUEST, 
                                   ReqGrantUserRole.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "grantRole", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String user = payload.user;
         String roleName = payload.roleName;
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantRole", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantRole", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Assign the role to the user.
         int rows = 0;
         try {rows = getUserImpl().grantRole(threadContext.getTenantId(), 
                                             threadContext.getUser(), user, roleName);
         }
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint, "Role", roleName);
             }
         
         // Populate the response.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " roles assigned"), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* removeRole:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/removeRole")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Remove a previously granted role from a user.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveUserRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role removed from user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response removeRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "removeRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqRemoveUserRole payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_REMOVE_USER_ROLE_REQUEST, 
                                   ReqRemoveUserRole.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "removeRole", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String user = payload.user;
         String roleName = payload.roleName;
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantRole", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantRole", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Remove the role from the user.
         int rows = 0;
         try {rows = getUserImpl().removeRole(threadContext.getTenantId(), user, roleName);}
             catch (TapisNotFoundException e) {
                 // Remove calls are idempotent so we simply log the
                 // occurrence and let normal processing take place.
                 _log.warn(e.getMessage());
             }
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_REMOVE_USER_ROLE_ERROR",  
                                              threadContext.getTenantId(), roleName, user,
                                              e.getMessage());
                 return getExceptionResponse(e, msg, prettyPrint);
             }
         
         // Populate the response.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " roles removed"), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantRoleWithPermission:                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantRoleWithPerm")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified role containing the specified permission.  "
                         + "This compound request first adds the permission to the role if it is not "
                         + "already a member of the role and then assigns the role "
                         + "to the user.  The change count returned can range from zero to two "
                         + "depending on how many insertions were actually required.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRoleWithPermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission assigned to user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Role not found.",
                      content = @Content(schema = @Schema(
                          implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response grantRoleWithPermission(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                             InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "grantRoleWithPermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqGrantUserRoleWithPermission payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_GRANT_USER_ROLE_WITH_PERM_REQUEST, 
                                   ReqGrantUserRoleWithPermission.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "grantRoleWithPermission", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
         // Fill in the parameter fields.
         String user     = payload.user;
         String roleName = payload.roleName;
         String permSpec = payload.permSpec;
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantPermission", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantPermission", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permSpec)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantPermission", "permSpec");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------        
         // Create the role and/or permission.
         int rows = 0;
         try {
             rows = getUserImpl().grantRoleWithPermission(threadContext.getTenantId(), 
                                                          threadContext.getUser(), 
                                                          user, roleName, permSpec);
         } 
             catch (Exception e) {
                 // We assume a bad request for all other errors.
                 String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", 
                                              threadContext.getTenantId(), threadContext.getUser(), 
                                              permSpec, roleName);
                 return getExceptionResponse(e, msg, prettyPrint, "Role", roleName);
             }

         // Populate the response.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;

         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " changes"), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* hasRole:                                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/hasRole")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user has been assigned the specified role, "
                           + "either directly or transitively.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response hasRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                             InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "hasRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqUserHasRole payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_USER_HAS_ROLE_REQUEST, 
                                   ReqUserHasRole.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "hasRole", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Make sure we have a role name.
         if (StringUtils.isBlank(payload.roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Repackage into a multi-role request.
         ReqUserHasRoleMulti multi = new ReqUserHasRoleMulti();
         multi.user = payload.user;
         multi.roleNames = new String[] {payload.roleName};
         
         // Call the real method.
         return hasRoleMulti(payloadStream, prettyPrint, AuthOperation.ANY, multi);
     }

     /* ---------------------------------------------------------------------------- */
     /* hasRoleAny:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/hasRoleAny")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user has been assigned any of the roles "
                           + "specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response hasRoleAny(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "hasRoleAny", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Call the real method.
         return hasRoleMulti(payloadStream, prettyPrint, AuthOperation.ANY, null);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* hasRoleAll:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/hasRoleAll")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user has been assigned all of the roles "
                           + "specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response hasRoleAll(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "hasRoleAll", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Call the real method.
         return hasRoleMulti(payloadStream, prettyPrint, AuthOperation.ALL, null);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* isPermitted:                                                                 */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/isPermitted")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether specified permission matches a permission "
                           + "assigned to the user, either directly or transitively.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermitted.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response isPermitted(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                 InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "isPermitted", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqUserIsPermitted payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_USER_IS_PERMITTED_REQUEST, 
                                   ReqUserIsPermitted.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "isPermitted", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Make sure we have a permission specification.
         if (StringUtils.isBlank(payload.permSpec)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermitted", "permSpec");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Transfer to a new payload object.
         ReqUserIsPermittedMulti multi = new ReqUserIsPermittedMulti();
         multi.user = payload.user;
         multi.permSpecs = new String[] {payload.permSpec};
         
         // Call the real method.
         return isPermittedMulti(payloadStream, prettyPrint, AuthOperation.ANY, multi);
     }

     /* ---------------------------------------------------------------------------- */
     /* isPermittedAny:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/isPermittedAny")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user's permissions satisfy any of the "
                           + "permission specifications contained in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response isPermittedAny(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "isPermittedAny", _request.getRequestURL());
             _log.trace(msg);
         }

         // Call the real method.
         return isPermittedMulti(payloadStream, prettyPrint, AuthOperation.ANY, null);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* isPermittedAll:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/isPermittedAll")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user's permissions satisfy all of the "
                           + "permission specifications contained in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response isPermittedAll(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "isPermittedAll", _request.getRequestURL());
             _log.trace(msg);
         }

         // Call the real method.
         return isPermittedMulti(payloadStream, prettyPrint, AuthOperation.ALL, null);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* getUsersWithRole:                                                            */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/withRole/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get all users assigned a role.  The role must exist in the tenant.",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Sorted list of users assigned a role.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getUsersWithRole(@PathParam("roleName") String roleName,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUsersWithRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Assign the role to the user.
         List<String> users = null;
         try {users = getUserImpl().getUsersWithRole(threadContext.getTenantId(), roleName);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint, "Role");
             }
         
         // Fill in the response.
         RespNameArray names = new RespNameArray();
         String[] array = new String[users.size()];
         names.names = users.toArray(array);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUsersWithPermission:                                                      */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/withPermission/{permSpec}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = 
               "Get all users assigned a permission.  " +
               "The permSpec parameter is a permission specification " +
               "that uses colons as separators, the asterisk as a wildcard character and " +
               "commas to define lists.  Here are examples of permission specifications:\n\n" +
               "" +
               "    system:mytenant:read:mysystem\n" +
               "    system:mytenant:*:mysystem\n" +
               "    system:mytenant\n" +
               "    files:mytenant:read,write:mysystems\n" +
               "" +
               "This method recognizes the percent sign (%) as a string wildcard only " + 
               "in the context of database searching.  If a percent sign appears in the " +
               "permSpec it is interpreted as a zero or more character wildcard.  For example, " +
               "the following specification would match the first three of the above " +
               "example specifications but not the fourth:\n\n" +
               "" +
               "    system:mytenant:%\n",

             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Sorted list of users assigned a permission.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response getUsersWithPermission(@PathParam("permSpec") String permSpec,
                                            @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUsersWithPermission", _request.getRequestURL());
             _log.trace(msg);
         }

         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Assign the role to the user.
         List<String> users = null;
         try {users = getUserImpl().getUsersWithPermission(threadContext.getTenantId(), permSpec);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Fill in the response.
         RespNameArray names = new RespNameArray();
         String[] array = new String[users.size()];
         names.names = users.toArray(array);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, names)).build();
     }

     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* hasRoleMulti:                                                                */
     /* ---------------------------------------------------------------------------- */
     private Response hasRoleMulti(InputStream payloadStream, boolean prettyPrint, 
                                   AuthOperation op, ReqUserHasRoleMulti payload)
     {
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         if (payload == null) {
             try {payload = getPayload(payloadStream, FILE_SK_USER_HAS_ROLE_MULTI_REQUEST, 
                                       ReqUserHasRoleMulti.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "hasRoleMulti", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         }
         
         // Unpack inputs for convenience.
         String   user = payload.user;
         String[] roleNames = payload.roleNames;
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (roleNames == null || (roleNames.length == 0)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "roleNames");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         boolean authorized;
         try {authorized = getUserImpl().hasRole(threadContext.getTenantId(), user, 
                                                 roleNames, op);}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                              threadContext.getTenantId(), 
                                              user, e.getMessage());
                 return getExceptionResponse(e, msg, prettyPrint);
             }
         
         // Set the result payload.
         RespAuthorized authResp = new RespAuthorized();
         authResp.isAuthorized = authorized;
         
         // Set the response message.
         String resultCode;
         if (authorized) resultCode = "TAPIS_AUTHORIZED"; 
           else resultCode = "TAPIS_NOT_AUTHORIZED";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role.
         String respMsg = user + " authorized: " + authorized;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg(resultCode, "User", respMsg), prettyPrint, authResp)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* isPermittedMulti:                                                            */
     /* ---------------------------------------------------------------------------- */
     private Response isPermittedMulti(InputStream payloadStream, boolean prettyPrint, 
                                       AuthOperation op, ReqUserIsPermittedMulti payload)
     {
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         if (payload == null) {
             try {payload = getPayload(payloadStream, FILE_SK_USER_IS_PERMITTED_MULTI_REQUEST, 
                                       ReqUserIsPermittedMulti.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "isPermittedMulti", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         }
         
         // Unpack inputs for convenience.
         String   user      = payload.user;
         String[] permSpecs = payload.permSpecs;
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermittedMulti", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (permSpecs == null || (permSpecs.length == 0)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermittedMulti", "permSpecs");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         boolean authorized;
         try {authorized = getUserImpl().isPermitted(threadContext.getTenantId(), 
                                                     user, permSpecs, op);
         }
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_USER_GET_PERMISSIONS_ERROR", 
                                              threadContext.getTenantId(), user, e.getMessage());
                 return getExceptionResponse(e, msg, prettyPrint);
             }
         
         // Set the result payload.
         RespAuthorized authResp = new RespAuthorized();
         authResp.isAuthorized = authorized;
         
         // Set the response message.
         String resultCode;
         if (authorized) resultCode = "TAPIS_AUTHORIZED"; 
           else resultCode = "TAPIS_NOT_AUTHORIZED";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role.
         String respMsg = user + " authorized: " + authorized;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg(resultCode, "User", respMsg), prettyPrint, authResp)).build();
     }
     
}

package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.List;

import javax.annotation.security.PermitAll;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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

import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserPermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRoleWithPermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRevokeUserPermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRevokeUserRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermitted;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl.AuthOperation;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultAuthorized;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
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
    private static final String FILE_SK_GRANT_USER_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/GrantUserPermRequest.json";
    private static final String FILE_SK_REVOKE_USER_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RevokeUserPermRequest.json";
    private static final String FILE_SK_REVOKE_USER_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RevokeUserRoleRequest.json";
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
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response getUserNames(@QueryParam("tenant") String tenant,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUserNames", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         if (StringUtils.isBlank(tenant)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tenant");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         List<String> users = null;
         try {users = getUserImpl().getUserNames(tenant);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Populate response.
         ResultNameArray names = new ResultNameArray();
         String[] array = new String[users.size()];
         names.names = users.toArray(array);
         RespNameArray r = new RespNameArray(names);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " users"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUserRoles:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/roles/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the roles assigned to a user in the specified tenant, "
                     + "including those assigned transively.",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of roles names assigned to the user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response getUserRoles(@PathParam("user") String user,
                                  @QueryParam("tenant") String tenant,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUserRoles", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         if (StringUtils.isBlank(tenant)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tenant");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict read access to a user's roles.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         List<String> roles = null;
         try {roles = getUserImpl().getUserRoles(tenant, user);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Populate response.
         ResultNameArray names = new ResultNameArray();
         String[] array = new String[roles.size()];
         names.names = roles.toArray(array);
         RespNameArray r = new RespNameArray(names);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Roles", cnt + " roles"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUserPerms:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/perms/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the permissions assigned to a user in a tenant, "
                     + "including those assigned transively.  The result list can be "
                     + "optionally filtered by the one or both of the query "
                     + "parameters: implies and impliedBy.\n\n"
                     + ""
                     + "The implied parameter removes permissions from the result list "
                     + "that the specified permission do not imply. The impliedBy parameter "
                     + "removes permissions from the result list that the specified permission "
                     + "are not implied by. Below are some examples."
                     + ""
                     + "Consider a user that is assigned these permissions:\n\n"
                     + ""
                     + "    stream:dev:read:project1\n"
                     + "    stream:dev:read,write:project1\n"
                     + "    stream:dev:read,write,exec:project1\n\n"
                     + ""
                     + "**Using the *implies* Query Parameter**\n\n"
                     + ""
                     + "When _implies=stream:dev:*:project1_, this endpoint returns:\n\n"
                     + ""
                     + "    stream:dev:read:project1\n"
                     + "    stream:dev:read,write:project1\n"
                     + "    stream:dev:read,write,exec:project1\n\n"
                     + ""
                     + "When _implies=stream:dev:write:project1_, this endpoint returns an empty list.\n\n"
                     + ""
                     + "**Using the *impliedBy* Query Parameter**\n\n"
                     + ""
                     + "When _impliedBy=stream:dev:*:project1_, this endpoint returns an empty list.\n\n"
                     + ""
                     + "When _impliedBy=stream:dev:write:project1_, this endpoint returns:\n\n"
                     + ""
                     + "    stream:dev:read,write:project1\n"
                     + "    stream:dev:read,write,exec:project1\n\n"
                     + "",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of permissions assigned to the user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response getUserPerms(@PathParam("user") String user,
                                  @QueryParam("tenant") String tenant,
                                  @DefaultValue("") @QueryParam("implies") String implies,
                                  @DefaultValue("") @QueryParam("impliedBy") String impliedBy,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUserPerms", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         if (StringUtils.isBlank(tenant)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tenant");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict read access to a user's permissions.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         List<String> perms = null;
         try {
             perms = getUserImpl().getUserPerms(tenant, user, implies, impliedBy);
         }
         catch (Exception e) {
             return getExceptionResponse(e, null, prettyPrint);
         }
         
         // Populate response.
         ResultNameArray names = new ResultNameArray();
         String[] array = new String[perms.size()];
         names.names = perms.toArray(array);
         RespNameArray r = new RespNameArray(names);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Permissions", cnt + " permissions"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantRole:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantRole")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified role.  A valid tenant and user "
                     + "must be specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role assigned to user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user   = payload.user;
         String roleName = payload.roleName;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict which users can be granted a role.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // The requestor will always be non-null after the above check. 
         String requestor = TapisThreadLocal.tapisThreadContext.get().getJwtUser();
         
         // Assign the role to the user.
         int rows = 0;
         try {rows = getUserImpl().grantRole(tenant, requestor, user, roleName);
         }
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint, "Role", roleName);
             }
         
         // Populate the response.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " roles assigned"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* revokeUserRole:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/revokeUserRole")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Revoke a previously granted role from a user. No action "
                     + "is taken if the user is not currently assigned the role. "
                     + "This request is idempotent.\n\n"
                     + ""
                     + "A valid tenant and user must be specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRevokeUserRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role removed from user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response revokeUserRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "revokeUserRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqRevokeUserRole payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_REVOKE_USER_ROLE_REQUEST, 
                                   ReqRevokeUserRole.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "revokeUserRole", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user   = payload.user;
         String roleName = payload.roleName;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict from which users a role can be revoked.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Remove the role from the user.
         int rows = 0;
         try {rows = getUserImpl().revokeUserRole(tenant, user, roleName);}
             catch (TapisNotFoundException e) {
                 // Remove calls are idempotent so we simply log the
                 // occurrence and let normal processing take place.
                 _log.warn(e.getMessage());
             }
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_REMOVE_USER_ROLE_ERROR",  
                                              tenant, roleName, user, e.getMessage());
                 return getExceptionResponse(e, msg, prettyPrint);
             }
         
         // Populate the response.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " roles revoked"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantUserPermission:                                                         */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantUserPermission")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified permission by assigning that permission to "
                     + "to the user's default role.  If the user's default role does not exist,"
                     + "this request will create that role and grant it to the user before "
                     + "assigning the permission to the role.\n\n"
                     + ""
                     + "A user's default role name is discoverable by calling either of the "
                     + "user/defaultRole or role/defaultRole endpoints.\n\n"
                     + ""
                     + "The change count returned can range from zero to three "
                     + "depending on how many insertions and updates were actually required.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserPermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission assigned to user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response grantUserPermission(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
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
         ReqGrantUserPermission payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_GRANT_USER_PERM_REQUEST, 
                                   ReqGrantUserPermission.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "grantUserPermission", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
         // Fill in the parameter fields.
         String tenant   = payload.tenant;
         String user     = payload.user;
         String permSpec = payload.permSpec;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict to which users a permission can be granted.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // The requestor will always be non-null after the above check. 
         String requestor = TapisThreadLocal.tapisThreadContext.get().getJwtUser();

         // Create the role and/or permission.
         int rows = 0;
         try {
             rows = getUserImpl().grantUserPermission(tenant, requestor, user, permSpec);
         } 
             catch (Exception e) {
                 // We assume a bad request for all other errors.
                 String msg = MsgUtils.getMsg("SK_ADD_USER_PERMISSION_ERROR", 
                                              tenant, requestor, permSpec, user);
                 return getExceptionResponse(e, msg, prettyPrint);
             }

         // Populate the response.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);

         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " changes"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* revokeUserPermission:                                                        */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/revokeUserPermission")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Revoke the specified permission from the user's default role. "
                     + "A user's default role is constructed by prepending '$$' to the "
                     + "user's name. Default roles are created on demand. If the "
                     + "role does not exist when this method is called no error is "
                     + "reported and no changes occur.\n\n"
                     + ""
                     + "The change count returned can be zero or one "
                     + "depending on how many permissions were revoked.\n\n"
                     + ""
                     + "A valid tenant and user must be specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRevokeUserPermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission assigned to user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response revokeUserPermission(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                          InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "revokeUserPermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqRevokeUserPermission payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_REVOKE_USER_PERM_REQUEST, 
                                   ReqRevokeUserPermission.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "revokeUserPermission", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
         // Fill in the parameter fields.
         String tenant   = payload.tenant;
         String user     = payload.user;
         String permSpec = payload.permSpec;
         
         // Construct the user's default role name.
         String roleName = getRoleImpl().getUserDefaultRolename(user);
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict from which users a permission can be revoked.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------        
         // Remove the permission from the role.
         int rows = 0;
         try {
             rows = getRoleImpl().removeRolePermission(tenant, roleName, permSpec);
         } catch (TapisNotFoundException e) {
             // Default role not found is not considered an error.
         } catch (Exception e) {
             // A real error.
             String msg = MsgUtils.getMsg("SK_REMOVE_PERMISSION_ERROR", 
                                          tenant, user, permSpec, roleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role", roleName);
         }
    
         // Populate the response.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);

         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " changes"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantRoleWithPermission:                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantRoleWithPerm")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified role containing the specified permission.  "
                         + "This compound request first adds the permission to the role if it is not "
                         + "already a member of the role and then assigns the role "
                         + "to the user.  The change count returned can range from zero to two "
                         + "depending on how many insertions were actually required.\n\n"
                         + ""
                         + "A valid tenant and user must be specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRoleWithPermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission assigned to user.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Role not found.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
         // Fill in the parameter fields.
         String tenant   = payload.tenant;
         String user     = payload.user;
         String roleName = payload.roleName;
         String permSpec = payload.permSpec;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict to which users a permission can be granted.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------        
         // The requestor will always be non-null after the above check. 
         String requestor = TapisThreadLocal.tapisThreadContext.get().getJwtUser();
         
         // Create the role and/or permission.
         int rows = 0;
         try {
             rows = getUserImpl().grantRoleWithPermission(tenant, requestor, 
                                                          user, roleName, permSpec);
         } 
             catch (Exception e) {
                 // We assume a bad request for all other errors.
                 String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", 
                                              tenant, requestor, permSpec, roleName);
                 return getExceptionResponse(e, msg, prettyPrint, "Role", roleName);
             }

         // Populate the response.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);

         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", rows + " changes"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* hasRole:                                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/hasRole")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user in a tenant has been assigned "
                     + "the specified role, either directly or transitively.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Repackage into a multi-role request.
         ReqUserHasRoleMulti multi = new ReqUserHasRoleMulti();
         multi.tenant = payload.tenant;
         multi.user   = payload.user;
         multi.roleNames = new String[] {payload.roleName};
         
         // Call the real method.
         return hasRoleMulti(payloadStream, prettyPrint, AuthOperation.ANY, multi);
     }

     /* ---------------------------------------------------------------------------- */
     /* hasRoleAny:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/hasRoleAny")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user in a tenant has been assigned "
                     + "any of the roles specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user in a tenant has been assigned "
                     + "all of the roles specified in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether specified permission matches a permission "
                           + "assigned to the user, either directly or transitively.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermitted.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Transfer to a new payload object.
         ReqUserIsPermittedMulti multi = new ReqUserIsPermittedMulti();
         multi.tenant = payload.tenant;
         multi.user   = payload.user;
         multi.permSpecs = new String[] {payload.permSpec};
         
         // Call the real method.
         return isPermittedMulti(payloadStream, prettyPrint, AuthOperation.ANY, multi);
     }

     /* ---------------------------------------------------------------------------- */
     /* isPermittedAny:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/isPermittedAny")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user's permissions satisfy any of the "
                           + "permission specifications contained in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user's permissions satisfy all of the "
                           + "permission specifications contained in the request body.",
             tags = "user",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Check completed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
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
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response getUsersWithRole(@PathParam("roleName") String roleName,
                                      @QueryParam("tenant") String tenant,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUsersWithRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         if (StringUtils.isBlank(tenant)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tenant");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict which users can make this call.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Assign the role to the user.
         List<String> users = null;
         try {users = getUserImpl().getUsersWithRole(tenant, roleName);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint, "Role");
             }
         
         // Fill in the response.
         ResultNameArray names = new ResultNameArray();
         String[] array = new String[users.size()];
         names.names = users.toArray(array);
         RespNameArray r = new RespNameArray(names);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUsersWithPermission:                                                      */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/withPermission/{permSpec}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = 
               "Get all users in a tenant assigned a permission.  " +
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
               "in the context of database searching.  If a percent sign (%) appears in the " +
               "permSpec it is interpreted as a zero or more character wildcard.  For example, " +
               "the following specification would match the first three of the above " +
               "example specifications but not the fourth:\n\n" +
               "" +
               "    system:mytenant:%\n\n"
               + ""
               + "The wildcard character cannot appear as the first character in the permSpec.",

             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Sorted list of users assigned a permission.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response getUsersWithPermission(@PathParam("permSpec") String permSpec,
                                            @QueryParam("tenant") String tenant,
                                            @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getUsersWithPermission", _request.getRequestURL());
             _log.trace(msg);
         }

         // ------------------------- Input Processing -------------------------
         if (StringUtils.isBlank(tenant)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tenant");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict which users can make this call.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Assign the role to the user.
         List<String> users = null;
         try {users = getUserImpl().getUsersWithPermission(tenant, permSpec);}
             catch (Exception e) {
                 return getExceptionResponse(e, null, prettyPrint);
             }
         
         // Fill in the response.
         ResultNameArray names = new ResultNameArray();
         String[] array = new String[users.size()];
         names.names = users.toArray(array);
         RespNameArray r = new RespNameArray(names);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getDefaultUserRole:                                                          */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/defaultRole/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @PermitAll
     @Operation(
             description = 
               "Get a user's default role. The default role can be explicitly created "
               + "by a POST call or implicitly by the system whenever it's needed and "
               + "it doesn't already exist. "
               + ""
               + "A user's default role is *currently* constructed by prepending '$$' to the "
               + "user's name.  This implies the maximum length of a user name is 58 since "
               + "role names are limited to 60 characters.\n\n"
               + ""
               + "Since the default role name may be constructed differently in the future, "
               + "this API is the recommended way to determine the default role."
               + "",

             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "The user's default role name.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response getDefaultUserRole(@PathParam("user") String user,
                                        @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getDefaultUserRole", _request.getRequestURL());
             _log.trace(msg);
         }

         // ------------------------- Input Processing -------------------------
         // Check input.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDefaultUserRole", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (user.length() > UserImpl.MAX_USER_NAME_LEN) {
             String msg = MsgUtils.getMsg("SK_USER_NAME_LEN", "anyTenant", 
                                          user, UserImpl.MAX_USER_NAME_LEN);
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         // Construct the role name.
         String name = null;
         try {name = getUserImpl().getUserDefaultRolename(user);}
         catch (Exception e) {
             return getExceptionResponse(e, null, prettyPrint);
         }
         
         // Fill in the response.
         ResultName dftName = new ResultName();
         dftName.name = name;
         RespName r = new RespName(dftName);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Role", name), prettyPrint, r)).build();
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
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         }
         
         // Unpack inputs for convenience.
         String   tenant    = payload.tenant;
         String   user      = payload.user;
         String[] roleNames = payload.roleNames;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict the users that can be queried.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         boolean authorized;
         try {authorized = getUserImpl().hasRole(tenant, user, roleNames, op);}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                              tenant, user, e.getMessage());
                 return getExceptionResponse(e, msg, prettyPrint);
             }
         
         // Set the result payload.
         ResultAuthorized authResp = new ResultAuthorized();
         authResp.isAuthorized = authorized;
         RespAuthorized r = new RespAuthorized(authResp);
         
         // Set the response message.
         String resultCode;
         if (authorized) resultCode = "TAPIS_AUTHORIZED"; 
           else resultCode = "TAPIS_NOT_AUTHORIZED";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role.
         String respMsg = user + " authorized: " + authorized;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg(resultCode, "User", respMsg), prettyPrint, r)).build();
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
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         }
         
         // Unpack inputs for convenience.
         String   tenant    = payload.tenant;
         String   user      = payload.user;
         String[] permSpecs = payload.permSpecs;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.  By passing in a null
         // user we do not restrict the users that can be queried.
         Response resp = checkTenantUser(tenant, null, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         boolean authorized;
         try {authorized = getUserImpl().isPermitted(tenant, user, permSpecs, op);
         }
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_USER_GET_PERMISSIONS_ERROR", 
                                              tenant, user, e.getMessage());
                 return getExceptionResponse(e, msg, prettyPrint);
             }
         
         // Set the result payload.
         ResultAuthorized authResp = new ResultAuthorized();
         authResp.isAuthorized = authorized;
         RespAuthorized r = new RespAuthorized(authResp);
         
         // Set the response message.
         String resultCode;
         if (authorized) resultCode = "TAPIS_AUTHORIZED"; 
           else resultCode = "TAPIS_NOT_AUTHORIZED";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role.
         String respMsg = user + " authorized: " + authorized;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg(resultCode, "User", respMsg), prettyPrint, r)).build();
     }
}

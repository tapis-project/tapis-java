package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;

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

import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRoleWithPermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqGrantUserRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveUserRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserHasRoleMulti;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermitted;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUserIsPermittedMulti;
import edu.utexas.tacc.tapis.security.api.responseBody.RespAuthorized;
import edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
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
    /*                                     Enums                                    */
    /* **************************************************************************** */
    // Logical operations applied during authentication.
    private enum AuthOperation {ANY, ALL}
    
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
                           + "have been granted a permission.",
             tags = "user",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of user names returned.",
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
         
         // ***** DUMMY TEST Response Data
         RespNameArray names = new RespNameArray();
         names.names = new String[2];
         names.names[0] = "bud";
         names.names[1] = "harry";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = (names == null || names.names == null) ? 0 : names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUserRoles:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/roles/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the roles assigned to a user.",
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
         
         // ***** DUMMY TEST Response Data
         RespNameArray names = new RespNameArray();
         names.names = new String[2];
         names.names[0] = "role1";
         names.names[1] = "role2";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = (names == null || names.names == null) ? 0 : names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getUserPerms:                                                                */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/perms/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Get the roles assigned to a user.",
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
         
         // ***** DUMMY TEST Response Data
         RespNameArray names = new RespNameArray();
         names.names = new String[2];
         names.names[0] = "perm1";
         names.names[1] = "perm2";
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = (names == null || names.names == null) ? 0 : names.names.length;
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Users", cnt + " items"), prettyPrint, names)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantRole:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantRole")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified role using either a request body "
                         + "or query parameters, but not both.",
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
                  @ApiResponse(responseCode = "404", description = "Named resource not found."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response grantRole(@QueryParam("user") String user,
                               @QueryParam("roleName") String roleName,
                               @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                               InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "grantRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(user, roleName)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "user, roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (user == null) {
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
             user = payload.user;
             roleName = payload.roleName;
         }
         
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
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** user = " + user);
         System.out.println("***** roleName = " + roleName);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 1;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", user), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* removeRole:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/removeRole")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Remove a previously granted role from a user using either a request body "
                         + "or query parameters, but not both.",
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
     public Response removeRole(@QueryParam("user") String user,
                                @QueryParam("roleName") String roleName,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "removeRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(user, roleName)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "user, roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (user == null) {
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
             user = payload.user;
             roleName = payload.roleName;
         }
         
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
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** user = " + user);
         System.out.println("***** roleName = " + roleName);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 1;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", user), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* grantRoleWithPermission:                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/grantRoleWithPerm")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Grant a user the specified role containing the specified permission "
                         + "using either a request body or query parameters, but not both.  This "
                         + "compound request first adds the permission to the role it is not "
                         + "already a member of the role, and then the request assigns the role "
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
                  @ApiResponse(responseCode = "404", description = "Named resource not found."),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response grantRoleWithPermission(@QueryParam("user") String user,
                                             @QueryParam("roleName") String roleName,
                                             @QueryParam("permSpec") String permSpec,
                                             @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                             InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "grantRoleWithPermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(user, roleName, permSpec)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "user, roleName, permSpec");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (user == null) {
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
             user = payload.user;
             roleName = payload.roleName;
             permSpec = payload.permSpec;
         }
         
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
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** user = " + user);
         System.out.println("***** roleName = " + roleName);
         System.out.println("***** roleName = " + permSpec);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 1;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "User", user), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* hasRole:                                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/hasRole")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user has been assigned the specified role "
                           + "using either a request body or query parameters, but not both.",
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
     public Response hasRole(@QueryParam("user") String user,
                             @QueryParam("roleName") String roleName,
                             @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                             InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "hasRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(user, roleName)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "user, roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (user == null) {
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
             
             // Fill in the parameter fields.
             user = payload.user;
             roleName = payload.roleName;
         }
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** user = " + user);
         System.out.println("***** roleName = " + roleName);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespAuthorized authResp = new RespAuthorized();
         authResp.isAuthorized = true;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_AUTHORIZED", "User", user), prettyPrint, authResp)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* isPermitted:                                                                 */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/isPermitted")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Check whether a user has been assigned the specified role "
                           + "using either a request body or query parameters, but not both.",
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
     public Response isPermitted(@QueryParam("user") String user,
                                 @QueryParam("permSpec") String permSpec,
                                 @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                 InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "isPermitted", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some query parameters.
         if (!allNullOrNot(user, permSpec)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "user, permSpec");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (user == null) {
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
             
             // Fill in the parameter fields.
             user = payload.user;
             permSpec = payload.permSpec;
         }
         
         // Final checks.
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermitted", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permSpec)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermitted", "permSpec");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** user = " + user);
         System.out.println("***** permSpec = " + permSpec);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespAuthorized authResp = new RespAuthorized();
         authResp.isAuthorized = true;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_AUTHORIZED", "User", user), prettyPrint, authResp)).build();
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
         return hasRoleMulti(payloadStream, prettyPrint, AuthOperation.ANY);
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
                                          "hasRoleAny", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Call the real method.
         return hasRoleMulti(payloadStream, prettyPrint, AuthOperation.ALL);
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
         return isPermittedMulti(payloadStream, prettyPrint, AuthOperation.ANY);
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
         return isPermittedMulti(payloadStream, prettyPrint, AuthOperation.ALL);
     }
     
     /* **************************************************************************** */
     /*                                Public Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* hasRoleMulti:                                                                */
     /* ---------------------------------------------------------------------------- */
     private Response hasRoleMulti(InputStream payloadStream, boolean prettyPrint, 
                                   AuthOperation op)
     {
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqUserHasRoleMulti payload = null;
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
         
         // Final checks.
         if (StringUtils.isBlank(payload.user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (payload.roleNames == null || (payload.roleNames.length == 0)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "roleNames");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------

         // ***** DUMMY TEST Code
         System.out.println("***** user = " + payload.user);
         System.out.println("***** roleNames = " + StringUtils.join(payload.roleNames, ", "));
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespAuthorized authResp = new RespAuthorized();
         authResp.isAuthorized = true;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_AUTHORIZED", "User", payload.user), prettyPrint, authResp)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* isPermittedMulti:                                                            */
     /* ---------------------------------------------------------------------------- */
     private Response isPermittedMulti(InputStream payloadStream, boolean prettyPrint, 
                                       AuthOperation op)
     {
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqUserIsPermittedMulti payload = null;
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
         
         // Final checks.
         if (StringUtils.isBlank(payload.user)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermittedMulti", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (payload.permSpecs == null || (payload.permSpecs.length == 0)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermittedMulti", "permSpecs");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------

         // ***** DUMMY TEST Code
         System.out.println("***** user = " + payload.user);
         System.out.println("***** permSpecs = " + StringUtils.join(payload.permSpecs, ", "));
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespAuthorized authResp = new RespAuthorized();
         authResp.isAuthorized = true;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_AUTHORIZED", "User", payload.user), prettyPrint, authResp)).build();
     }
}

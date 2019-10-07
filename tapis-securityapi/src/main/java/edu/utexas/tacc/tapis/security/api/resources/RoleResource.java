package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.List;

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
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveChildRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveRolePermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqReplacePathPrefix;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleDescription;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleName;
import edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount;
import edu.utexas.tacc.tapis.security.api.responseBody.RespName;
import edu.utexas.tacc.tapis.security.api.responseBody.RespNameArray;
import edu.utexas.tacc.tapis.security.api.responseBody.RespResourceUrl;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleTreeDao;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
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
    private static final String FILE_SK_REMOVE_CHILD_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RemoveChildRoleRequest.json";
    private static final String FILE_SK_REPLACE_PATH_PREFIX_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/ReplacePathPrefixRequest.json";

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
             description = "Get the names of all roles in the tenant in alphabetic order.",
             tags = "role",
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
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the dao.
         SkRoleDao dao = null;
         try {dao = getSkRoleDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         List<String> list = null;
         try {
             list = dao.getRoleNames(threadContext.getTenantId());
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_GET_NAMES_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Assign result.
         RespNameArray names = new RespNameArray();
         names.names = list.toArray(new String[list.size()]);

         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
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
         tags = "role",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Named role returned.",
               content = @Content(schema = @Schema(
                   implementation = edu.utexas.tacc.tapis.security.authz.model.SkRole.class))),
              @ApiResponse(responseCode = "400", description = "Input error."),
              @ApiResponse(responseCode = "401", description = "Not authorized."),
              @ApiResponse(responseCode = "404", description = "Named role not found.",
                content = @Content(schema = @Schema(
                   implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
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
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the dao.
         SkRoleDao dao = null;
         try {dao = getSkRoleDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         SkRole role = null;
         try {
             role = dao.getRole(threadContext.getTenantId(), roleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_GET_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Adjust status based on whether we found the role.
         if (role == null) {
             RespName missingName = new RespName();
             missingName.name = roleName;
             return Response.status(Status.NOT_FOUND).entity(RestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_FOUND", "Role", roleName), prettyPrint, missingName)).build();
         }
         
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
             description = "Create a role using either a request body or query parameters, "
                           + "but not both.  Role names are case sensitive, alpha-numeric "
                           + "strings that can also contain underscores.  Role names must "
                           + "start with an alphbetic character and can be no more than 60 "
                           + "characters in length.  The desciption can be no more than "
                           + "2048 characters long.  If the role already exists, this "
                           + "request has no effect.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqCreateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role existed.",
                         content = @Content(schema = @Schema(
                                 implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespResourceUrl.class))),
                  @ApiResponse(responseCode = "201", description = "Role created.",
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
         if (!isValidName(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "createRole", "roleName");
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
         // Get the dao.
         SkRoleDao dao = null;
         try {dao = getSkRoleDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.createRole(threadContext.getTenantId(), threadContext.getUser(), 
                                   roleName, description);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_CREATE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // NOTE: We need to assign a location header as well.
         //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
         RespResourceUrl respUrl = new RespResourceUrl();
         respUrl.url = _request.getRequestURL().toString() + "/" + roleName;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we created the role. 
         return Response.status(Status.CREATED).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_CREATED", "Role", roleName), prettyPrint, respUrl)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* deleteRoleByName:                                                            */
     /* ---------------------------------------------------------------------------- */
     @DELETE
     @Path("/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Delete the named role.",
         tags = "role",
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
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the dao.
         SkRoleDao dao = null;
         try {dao = getSkRoleDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.deleteRole(threadContext.getTenantId(), roleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_DELETE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Return the number of row affected.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we deleted the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_DELETED", "Role", roleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* updateRoleName:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/updateName/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Update an existing role using either a request body or query parameters, "
                           + "but not both.  Role names are case sensitive, alphanumeric strings "
                           + "that can contain underscores but must begin with an alphabetic "
                           + "character.  The limit on role name is 60 characters.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleName.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role name updated."),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response updateRoleName(@PathParam("roleName") String roleName,
                                    @QueryParam("newRoleName") String newRoleName,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "updateRoleName", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // If all query parameters are null, we need to use the payload.
         if (newRoleName == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqUpdateRoleName payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_UPDATE_ROLE_REQUEST, 
                                       ReqUpdateRoleName.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "updateRoleName", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             newRoleName = payload.newRoleName;
         }
         
         // By this point there should be at least one non-null parameter.
         if (StringUtils.isBlank(newRoleName)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "newRoleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (!isValidName(newRoleName)) {
             String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "updateRoleName", "newRoleName");
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
         // Get the dao.
         SkRoleDao dao = null;
         try {dao = getSkRoleDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.updateRoleName(threadContext.getTenantId(), threadContext.getUser(), 
                                       roleName, newRoleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Did we update anything?
         if (rows == 0) {
             String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg);
             return Response.status(Status.NOT_FOUND).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* updateRoleDescription:                                                       */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/updateDesc/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Update an existing role using either a request body or query parameters, "
                           + "but not both. The limit on a description is 2048 characters.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleDescription.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role description updated."),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response updateRoleDescription(
                                @PathParam("roleName") String roleName,
                                @QueryParam("description") String description,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "updateRoleDescription", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // If all query parameters are null, we need to use the payload.
         if (description == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqUpdateRoleDescription payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_UPDATE_ROLE_REQUEST, 
                                       ReqUpdateRoleDescription.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "updateRoleName", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             description = payload.description;
         }
         
         // By this point there should be at least one non-null parameter.
         if (StringUtils.isBlank(description)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "description");
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
         // Get the dao.
         SkRoleDao dao = null;
         try {dao = getSkRoleDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.updateRoleDescription(
                                 threadContext.getTenantId(), threadContext.getUser(), 
                                 roleName, description);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Did we update anything?
         if (rows == 0) {
             String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          roleName);
             _log.error(msg);
             return Response.status(Status.NOT_FOUND).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* addRolePermission:                                                           */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/addPerm")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Add a permission to an existing role using either a request body "
                         + "or query parameters, but not both.  If the permission already exists, "
                         + "then the request has no effect and the change count returned is "
                         + "zero. Otherwise, the permission is added and the change count is one.",
             tags = "role",
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
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response addRolePermission(@QueryParam("roleName") String roleName,
                                       @QueryParam("permSpec") String permSpec,
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
         if (!allNullOrNot(roleName, permSpec)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "roleName, permSpec");
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
             permSpec = payload.permSpec;
         }
         
         // Final checks.
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permSpec)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "permSpec");
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
         // Get the dao.
         SkRolePermissionDao dao = null;
         try {dao = getSkRolePermissionDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.assignPermission(threadContext.getTenantId(), threadContext.getUser(), 
                                         roleName, permSpec);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          permSpec, roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Report the number of rows changed.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
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
             tags = "role",
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
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response removeRolePermission(@QueryParam("roleName") String roleName,
                                          @QueryParam("permSpec") String permSpec,
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
         if (!allNullOrNot(roleName, permSpec)) {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", "roleName, permSpec");
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
             permSpec = payload.permSpec;
         }
         
         // Final checks.
         if (StringUtils.isBlank(roleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "roleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(permSpec)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "permSpec");
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
         // Get the dao.
         SkRolePermissionDao dao = null;
         try {dao = getSkRolePermissionDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.removePermission(threadContext.getTenantId(), roleName, permSpec);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_REMOVE_PERMISSION_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          permSpec, roleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Report the number of rows changed.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
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
                         + "or query parameters, but not both.  If the child already exists, "
                         + "then the request has no effect and the change count returned is "
                         + "zero. Otherwise, the child is added and the change count is one.",
             tags = "role",
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
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
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
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the dao.
         SkRoleTreeDao dao = null;
         try {dao = getSkRoleTreeDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roleTree");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.assignChildRole(threadContext.getTenantId(), threadContext.getUser(), 
                                        parentRoleName, childRoleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ADD_CHILD_ROLE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          childRoleName, parentRoleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Report the number of rows changed.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", parentRoleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* removeChildRole:                                                             */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/removeChild")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Remove a child role from a parent role using either a request body "
                         + "or query parameters, but not both.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveChildRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Child removed from parent role.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response removeChildRole(@QueryParam("parentRoleName") String parentRoleName,
                                     @QueryParam("childRoleName") String childRoleName,
                                     @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                     InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "removeChildRole", _request.getRequestURL());
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
             ReqRemoveChildRole payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_REMOVE_CHILD_ROLE_REQUEST, 
                                       ReqRemoveChildRole.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "removeChildRole", e.getMessage());
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
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeChildRole", "parentRoleName");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(childRoleName)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeChildRole", "childRoleName");
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
         // Get the dao.
         SkRoleTreeDao dao = null;
         try {dao = getSkRoleTreeDao();}
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roleTree");
                 _log.error(msg, e);
                 return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the role.
         int rows = 0;
         try {
             rows = dao.removeChildRole(threadContext.getTenantId(),  
                                        parentRoleName, childRoleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_DELETE_CHILD_ROLE_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          childRoleName, parentRoleName);
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                 entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Report the number of rows changed.
         RespChangeCount count = new RespChangeCount();
         count.changes = rows;
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", parentRoleName), prettyPrint, count)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* replacePathPrefix:                                                           */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/replacePathPrefix")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Replace the text in a permission specification when its last component "
                         + "defines an *extended path attribute*.  Extended path attributes "
                         + "enhance the standard Shiro matching algorithm with one that treats "
                         + "designated components in a permission specification as a path name, "
                         + "such as a posix file or directory path name.  This request is useful "
                         + "when files or directories have been renamed or moved and their "
                         + "authorizations need to be adjusted.  Consider, for example, "
                         + "permissions that conform to the following specification:\n\n"
                         + ""
                         + "      store:tenantId:op:systemId:path\n\n"
                         + ""
                         + "By convention, the last component is an extended path attribute whose "
                         + "content can be changed by replacePathPrefix requests.  Specifically, paths "
                         + "that begin with the oldPrefix will have that prefix replaced with "
                         + "the newPrefix value.  Replacement only occurs on permissions "
                         + "that also match the schema and oldSystemId parameter values.  The systemId "
                         + "is required to be the next to last attribute and immediately preceding "
                         + "the path attribute.  The oldSystemId is replaced with the newSystemId "
                         + "when a match is found.  If a roleName is provided, then replacement is "
                         + "limited to permissions defined only in that role.  Otherwise, permissions "
                         + "in all roles that meet the other matching criteria will be considered.\n\n"
                         + ""
                         + "Either a request body or query parameters can be used on this request, "
                         + "but not both.  The response indicates the number of changed permission "
                         + "specifications.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = false,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqReplacePathPrefix.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Path prefixes replaced.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespChangeCount.class))),
                  @ApiResponse(responseCode = "400", description = "Input error."),
                  @ApiResponse(responseCode = "401", description = "Not authorized."),
                  @ApiResponse(responseCode = "404", description = "Named role not found.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responseBody.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.")}
         )
     public Response replacePathPrefix(@QueryParam("schema")   String schema,
                                       @QueryParam("roleName") String roleName,  // can be null
                                       @QueryParam("oldSystemId") String oldSystemId,
                                       @QueryParam("newSystemId") String newSystemId,
                                       @QueryParam("oldPrefix") String oldPrefix,
                                       @QueryParam("newPrefix") String newPrefix,
                                       @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                       InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "replacePathPrefix", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Either query parameters are used or the payload is used, but not a mixture
         // of the two.  Query parameters take precedence if all are assigned; it's an
         // error to supply only some of the required query parameters.
         if (!allNullOrNot(schema, oldSystemId, newSystemId) && 
             !allNullOrNot(newSystemId, oldPrefix, newPrefix)) 
         {
             String msg = MsgUtils.getMsg("NET_INCOMPLETE_QUERY_PARMS", 
                                         "schema, oldSystemId, newSystemId, oldPrefix, newPrefix");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // If all parameters are null, we need to use the payload.
         if (schema == null) {
             // Parse and validate the json in the request payload, which must exist.
             ReqReplacePathPrefix payload = null;
             try {payload = getPayload(payloadStream, FILE_SK_REPLACE_PATH_PREFIX_REQUEST, 
                                       ReqReplacePathPrefix.class);
             } 
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                              "replacePathPrefix", e.getMessage());
                 _log.error(msg, e);
                 return Response.status(Status.BAD_REQUEST).
                   entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Fill in the parameter fields.
             schema = payload.schema;
             roleName = payload.roleName;
             oldSystemId = payload.oldSystemId;
             newSystemId = payload.newSystemId;
             oldPrefix = payload.oldPrefix;
             newPrefix = payload.newPrefix;
         }
         
         // Final checks for required parameters.
         if (StringUtils.isBlank(schema)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "schema");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(oldSystemId)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "oldSystemId");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(newSystemId)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "newSystemId");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(oldPrefix)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "oldPrefix");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(newPrefix)) {
             String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "newPrefix");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Processing ------------------------
         
         // ***** DUMMY TEST Code
         System.out.println("***** schema = " + schema);
         System.out.println("***** roleName = " + roleName);
         System.out.println("***** oldSystemId = " + oldSystemId);
         System.out.println("***** newSystemId = " + newSystemId);
         System.out.println("***** oldPrefix = " + oldPrefix);
         System.out.println("***** newPrefix = " + newPrefix);
         // ***** END DUMMY TEST Code
         
         // ***** DUMMY RESPONSE Code
         RespChangeCount count = new RespChangeCount();
         count.changes = 2;
         // ***** END DUMMY RESPONSE Code
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Permission", oldPrefix), prettyPrint, count)).build();
     }
     
}

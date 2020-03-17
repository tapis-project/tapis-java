package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.List;

import javax.annotation.security.PermitAll;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import edu.utexas.tacc.tapis.security.api.requestBody.ReqPreviewPathPrefix;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveChildRole;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveRolePermission;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqReplacePathPrefix;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleDescription;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleName;
import edu.utexas.tacc.tapis.security.api.responses.RespPathPrefixes;
import edu.utexas.tacc.tapis.security.api.responses.RespRole;
import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.security.authz.permissions.PermissionTransformer.Transformation;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
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
    private static final String FILE_SK_UPDATE_ROLE_NAME_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UpdateRoleNameRequest.json";
    private static final String FILE_SK_UPDATE_ROLE_DESCRIPTION_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/UpdateRoleDescriptionRequest.json";
    private static final String FILE_SK_ADD_ROLE_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/AddRolePermissionRequest.json";
    private static final String FILE_SK_ADD_CHILD_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/AddChildRoleRequest.json";
    private static final String FILE_SK_REMOVE_ROLE_PERM_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RemoveRolePermissionRequest.json";
    private static final String FILE_SK_REMOVE_CHILD_ROLE_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/RemoveChildRoleRequest.json";
    private static final String FILE_SK_PREVIEW_PATH_PREFIX_REQUEST = 
            "/edu/utexas/tacc/tapis/security/api/jsonschema/PreviewPathPrefixRequest.json";
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
             description = "Get the names of all roles in the tenant in alphabetic order.\n\n"
                     + ""
                     + "A valid tenant must be specified as a query parameter.",
             tags = "role",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "List of role names returned.",
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
     public Response getRoleNames(@QueryParam("tenant") String tenant,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getRoleNames", _request.getRequestURL());
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
         // Create the role.
         List<String> list = null;
         try {
             list = getRoleImpl().getRoleNames(tenant);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_GET_NAMES_ERROR", tenant, 
                                          TapisThreadLocal.tapisThreadContext.get().getJwtUser());
             return getExceptionResponse(e, msg, prettyPrint);
         }
         
         // Assign result.
         ResultNameArray names = new ResultNameArray();
         names.names = list.toArray(new String[list.size()]);
         RespNameArray r = new RespNameArray(names);

         // ---------------------------- Success ------------------------------- 
         // Success means we found the tenant's role names.
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Roles", cnt + " items"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* getRoleByName:                                                               */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Get the named role's definition.  A valid tenant must be "
                       + "specified as a query parameter.",
         tags = "role",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Named role returned.",
               content = @Content(schema = @Schema(
                   implementation = edu.utexas.tacc.tapis.security.api.responses.RespRole.class))),
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
     public Response getRoleByName(@PathParam("roleName") String roleName,
                                   @QueryParam("tenant") String tenant,
                                   @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getRoleByName", _request.getRequestURL());
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
         // Create the role.
         SkRole role = null;
         try {
             role = getRoleImpl().getRoleByName(tenant, roleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_GET_ERROR", tenant,
                                          TapisThreadLocal.tapisThreadContext.get().getJwtUser(), 
                                          roleName);
             return getExceptionResponse(e, msg, prettyPrint);
         }

         // Adjust status based on whether we found the role.
         if (role == null) {
             ResultName missingName = new ResultName();
             missingName.name = roleName;
             RespName r = new RespName(missingName);
             return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_NOT_FOUND", "Role", roleName), prettyPrint, r)).build();
         }
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         RespRole r = new RespRole(role);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Role", roleName), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* createRole:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Create a role using a request body.  "
                           + "Role names are case sensitive, alpha-numeric "
                           + "strings that can also contain underscores.  Role names must "
                           + "start with an alphbetic character and can be no more than 58 "
                           + "characters in length.  The desciption can be no more than "
                           + "2048 characters long.  If the role already exists, this "
                           + "request has no effect.\n\n"
                           + ""
                           + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqCreateRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role existed.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl.class))),
                  @ApiResponse(responseCode = "201", description = "Role created.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl.class))),
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
     public Response createRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "createRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user = payload.user;
         String roleName = payload.roleName;
         String description = payload.description;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Create the role.
         int rows = 0;
         try {
             rows = getRoleImpl().createRole(tenant, user, roleName, description);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_CREATE_ERROR", tenant, user, roleName);
             return getExceptionResponse(e, msg, prettyPrint);
         }
         
         // NOTE: We need to assign a location header as well.
         //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
         ResultResourceUrl respUrl = new ResultResourceUrl();
         respUrl.url = _request.getRequestURL().toString() + "/" + roleName;
         RespResourceUrl r = new RespResourceUrl(respUrl);
         
         // ---------------------------- Success ------------------------------- 
         // Success means the role exists. 
         if (rows == 0)
             return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_EXISTED", "Role", roleName), prettyPrint, r)).build();
         else 
             return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_CREATED", "Role", roleName), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* deleteRoleByName:                                                            */
     /* ---------------------------------------------------------------------------- */
     @DELETE
     @Path("/{roleName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Delete the named role. A valid tenant and user must be "
                       + "specified as query parameters.",
         tags = "role",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Role deleted.",
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
     public Response deleteRoleByName(@PathParam("roleName") String roleName,
                                      @QueryParam("tenant") String tenant,
                                      @QueryParam("user") String user,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "deleteRoleByName", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         if (StringUtils.isBlank(tenant)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tenant");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         if (StringUtils.isBlank(user)) {
             String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "user");
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Delete the role.
         int rows = 0;
         try {
             rows =  getRoleImpl().deleteRoleByName(tenant, roleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_DELETE_ERROR", tenant, user, roleName);
             return getExceptionResponse(e, msg, prettyPrint);
         }
         
         // Return the number of row affected.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we deleted the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_DELETED", "Role", roleName), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* getRolePermissions:                                                          */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/{roleName}/perms")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
         description = "Get the named role's permissions.  By default, all permissions "
                 + "assigned to the role, whether directly and transitively through "
                 + "child roles, are returned.  Set the immediate query parameter to "
                 + "only retrieve permissions directly assigned to the role.  A valid "
                 + "tenant must be specified.",
         tags = "role",
         responses = 
             {@ApiResponse(responseCode = "200", description = "Named role returned.",
               content = @Content(schema = @Schema(
                   implementation = edu.utexas.tacc.tapis.security.api.responses.RespRole.class))),
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
     public Response getRolePermissions(@PathParam("roleName") String roleName,
                                        @QueryParam("tenant") String tenant,
                                        @DefaultValue("false") @QueryParam("immediate") boolean immediate,
                                        @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "getRolePermissions", _request.getRequestURL());
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
         // Create the role.
         List<String> list = null;
         try {
             list = getRoleImpl().getRolePermissions(tenant, roleName, immediate);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_GET_PERMISSIONS_ERROR",tenant, 
                                          TapisThreadLocal.tapisThreadContext.get().getJwtUser(), 
                                          roleName);
             return getExceptionResponse(e, msg, prettyPrint);
         }

         // Assign result.
         ResultNameArray names = new ResultNameArray();
         names.names = list.toArray(new String[list.size()]);
         RespNameArray r = new RespNameArray(names);

         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         int cnt = names.names.length;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_FOUND", "Permissions", cnt + " permissions"), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* updateRoleName:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/updateName/{roleName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Update an existing role using a request body.  "
                           + "Role names are case sensitive, alphanumeric strings "
                           + "that can contain underscores but must begin with an alphabetic "
                           + "character.  The limit on role name is 58 characters.\n\n"
                           + ""
                           + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleName.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role name updated.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
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
     public Response updateRoleName(@PathParam("roleName") String roleName,
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
         // Parse and validate the json in the request payload, which must exist.
         ReqUpdateRoleName payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_UPDATE_ROLE_NAME_REQUEST, 
                                   ReqUpdateRoleName.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "updateRoleName", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user   = payload.user;
         String newRoleName = payload.newRoleName;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Create the role.
         int rows = 0;
         try {
             rows = getRoleImpl().updateRoleName(tenant, user, roleName, newRoleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", tenant, user, roleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role");
         }
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* updateRoleDescription:                                                       */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/updateDesc/{roleName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Update an existing role using a request body.  "
                           + "The size limit on a description is 2048 characters.  "
                           + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqUpdateRoleDescription.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Role description updated.",
                      content = @Content(schema = @Schema(
                          implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
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
     public Response updateRoleDescription(
                                @PathParam("roleName") String roleName,
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
         // Parse and validate the json in the request payload, which must exist.
         ReqUpdateRoleDescription payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_UPDATE_ROLE_DESCRIPTION_REQUEST, 
                                   ReqUpdateRoleDescription.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "updateRoleName", e.getMessage());
              _log.error(msg, e);
              return Response.status(Status.BAD_REQUEST).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user = payload.user;
         String description = payload.description;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Create the role.
         int rows = 0;
         try {
             rows = getRoleImpl().updateRoleDescription(tenant, user, roleName, description);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", tenant, user, roleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role");
         }
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* addRolePermission:                                                           */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/addPerm")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Add a permission to an existing role using a request body.  "
                         + "If the permission already exists, "
                         + "then the request has no effect and the change count returned is "
                         + "zero. Otherwise, the permission is added and the change count is one.  "
                         + ""
                         + "Permissions are case-sensitive strings that follow the format "
                         + "defined by Apache Shiro (https://shiro.apache.org/permissions.html).  "
                         + "This format defines any number of colon-separated (:) parts, with the "
                         + "possible use of asterisks (*) as wildcards and commas (,) as "
                         + "aggregators.  Here are two example permission strings:\n\n"
                         + ""
                         + "    system:MyTenant:read,write:system1\n"
                         + "    system:MyTenant:create,read,write,delete:*\n\n"
                         + ""
                         + "See the Shiro documentation for further details.  Note that the three "
                         + "reserved characters, [: * ,], cannot appear in the text of any part.  "
                         + "It's the application's responsibility to escape those characters in "
                         + "a manner that is safe in the application's domain.\n\n"
                         + ""
                         + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqAddRolePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission assigned to role.",
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
     public Response addRolePermission(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                       InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "addRolePermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
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
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user = payload.user;
         String roleName = payload.roleName;
         String permSpec = payload.permSpec;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Add permission to role.
         int rows = 0;
         try {
             rows = getRoleImpl().addRolePermission(tenant, user, roleName, permSpec);
         } catch (Exception e) {
             // This only occurs when the role name is not found.
             String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", tenant, user, permSpec, roleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role", roleName);
         }

         // Report the number of rows changed.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* removeRolePermission:                                                        */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/removePerm")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Remove a permission from a role using a request body.  "
                     + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveRolePermission.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Permission removed from role.",
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
     public Response removeRolePermission(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                          InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "removeRolePermission", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant   = payload.tenant;
         String user     = payload.user;
         String roleName = payload.roleName;
         String permSpec = payload.permSpec;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Remove the permission from the role.
         int rows = 0;
         try {
             rows = getRoleImpl().removeRolePermission(tenant, roleName, permSpec);
         } catch (Exception e) {
             // Role not found is an error in this case.
             String msg = MsgUtils.getMsg("SK_REMOVE_PERMISSION_ERROR", 
                                          tenant, user, permSpec, roleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role", roleName);
         }
    
         // Report the number of rows changed.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", roleName), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* addChildRole:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/addChild")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Add a child role to another role using a request body.  "
                         + "If the child already exists, "
                         + "then the request has no effect and the change count returned is "
                         + "zero. Otherwise, the child is added and the change count is one.\n\n"
                         + ""
                         + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqAddChildRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Child assigned to parent role.",
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
     public Response addChildRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                  InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "addChildRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user   = payload.user;
         String parentRoleName = payload.parentRoleName;
         String childRoleName = payload.childRoleName;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Add the child role to the parent.
         int rows = 0;
         try {
             rows = getRoleImpl().addChildRole(tenant, user, parentRoleName, childRoleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_ADD_CHILD_ROLE_ERROR", 
                                          tenant, user, childRoleName, parentRoleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role");
         }

         // Report the number of rows changed.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", parentRoleName), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* removeChildRole:                                                             */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/removeChild")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Remove a child role from a parent role using a request body.  "
                     + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqRemoveChildRole.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Child removed from parent role.",
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
     public Response removeChildRole(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                     InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "removeChildRole", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
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
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user = payload.user;
         String parentRoleName = payload.parentRoleName;
         String childRoleName = payload.childRoleName;
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Create the role.
         int rows = 0;
         try {
             rows = getRoleImpl().removeChildRole(tenant, parentRoleName, childRoleName);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_DELETE_CHILD_ROLE_ERROR", 
                                          tenant, user, childRoleName, parentRoleName);
             return getExceptionResponse(e, msg, prettyPrint, "Role");
         }

         // Report the number of rows changed.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we found the role. 
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Role", parentRoleName), prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* previewPathPrefix:                                                           */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/previewPathPrefix")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "This read-only endpoint previews the transformations that would take "
                         + "place if the same input was used on a replacePathPrefix POST call. "
                         + "This call is also implemented as a POST so that the same input "
                         + "as used on replacePathPrefix can be used here, but this call changes "
                         + "nothing.\n\n"
                         + ""
                         + "This endpoint can be used to get an accounting of existing "
                         + "system/path combinations that match the input specification. "
                         + "Such information is useful when trying to duplicate a set of "
                         + "permissions. For example, one may want to copy a file subtree to "
                         + "another location and assign the same permissions to the new subtree "
                         + "as currently exist on the original subtree. One could use  "
                         + "this call to calculate the users that should be granted "
                         + "permission on the new subtree.\n\n"
                         + ""
                         + "The optional parameters are roleName, oldPrefix and newPrefix. "
                         + "No wildcards are defined for the path prefix parameters.  When "
                         + "roleName is specified then only permissions assigned to that role are "
                         + "considered.\n\n"
                         + ""
                         + "When the oldPrefix parameter is provided, it's used to filter out "
                         + "permissions whose paths do not begin with the specified string; when not "
                         + "provided, no path prefix filtering occurs.\n\n"
                         + ""
                         + "When the newPrefix parameter is not provided no new characters are "
                         + "prepended to the new path, effectively just removing the oldPrefix "
                         + "from the new path. "
                         + "When neither oldPrefix nor newPrefix are provided, no path transformation "
                         + "occurs, though system IDs can still be transformed.\n\n"
                         + ""
                         + "The result object contains an array of transformation objects, each of "
                         + "which contains the unique permission sequence number, the existing "
                         + "permission that matched the search criteria and the new permission if "
                         + "the specified transformations were applied.\n\n"
                         + ""
                         + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqPreviewPathPrefix.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Path prefixes previewed.",
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespPathPrefixes.class))),
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
     public Response previewPathPrefix(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                       InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "previewPathPrefix", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqPreviewPathPrefix payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_PREVIEW_PATH_PREFIX_REQUEST, 
                                   ReqPreviewPathPrefix.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "previewPathPrefix", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user = payload.user;
         String schema = payload.schema;
         String roleName = payload.roleName;
         String oldSystemId = payload.oldSystemId;
         String newSystemId = payload.newSystemId;
         String oldPrefix = payload.oldPrefix;
         String newPrefix = payload.newPrefix;
         
         // Canonicalize blank prefix values.
         if (StringUtils.isBlank(oldPrefix)) oldPrefix = "";
         if (StringUtils.isBlank(newPrefix)) newPrefix = "";
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
        // ------------------------ Request Processing ------------------------
         // Get the list of transformations that would be appled by replacePathPrefix.
         List<Transformation> transList = null;
         try {
                 transList = getRoleImpl().previewPathPrefix(schema, roleName, 
                                                             oldSystemId, newSystemId, 
                                                             oldPrefix, newPrefix, 
                                                             tenant);
             }
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_PERM_TRANSFORM_FAILED", schema, roleName,
                                              oldSystemId, oldPrefix, newSystemId, newPrefix,
                                              tenant);
                 _log.error(msg);
                 return Response.status(Status.BAD_REQUEST).
                         entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // Create the result object with a properly sized transformation array.
         var transArray = new Transformation[transList.size()];
         transArray = transList.toArray(transArray);
         RespPathPrefixes pathPrefixes = new RespPathPrefixes(transArray);
         
         // ---------------------------- Success ------------------------------- 
         // Success means we calculated zero or more transformations. 
         String s = oldSystemId + ":" + oldPrefix;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_READ", "Permission", s), prettyPrint, pathPrefixes)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* replacePathPrefix:                                                           */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/replacePathPrefix")
     @Consumes(MediaType.APPLICATION_JSON)
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
                         + "      files:tenantId:op:systemId:path\n\n"
                         + ""
                         + "By definition, the last component is an extended path attribute whose "
                         + "content can be changed by replacePathPrefix requests.  Specifically, paths "
                         + "that begin with the oldPrefix will have that prefix replaced with "
                         + "the newPrefix value.  Replacement only occurs on permissions "
                         + "that also match the schema and oldSystemId parameter values.  The systemId "
                         + "attribute is required to immediately precede the path attribute, which "
                         + "must be the last attribute.\n\n"
                         + ""
                         + "Additionally, the oldSystemId is replaced with the newSystemId "
                         + "when a match is found.  If a roleName is provided, then replacement is "
                         + "limited to permissions defined only in that role.  Otherwise, permissions "
                         + "in all roles that meet the other matching criteria will be considered.\n\n"
                         + ""
                         + "The optional parameters are roleName, oldPrefix and newPrefix. "
                         + "No wildcards are defined for the path prefix parameters.  When "
                         + "roleName is specified then only permissions assigned to that role are "
                         + "considered.\n\n"
                         + ""
                         + "When the oldPrefix parameter is provided, it's used to filter out "
                         + "permissions whose paths do not begin with the specified string; when not "
                         + "provided, no path prefix filtering occurs.\n\n"
                         + ""
                         + "When the newPrefix parameter is not provided no new characters are "
                         + "prepended to the new path, effectively just removing the oldPrefix "
                         + "from the new path. "
                         + "When neither oldPrefix nor newPrefix are provided, no path transformation "
                         + "occurs, though system IDs can still be transformed.\n\n"
                         + ""
                         + "The previewPathPrefix request provides a way to do a dry run using the "
                         + "same input as this request. The preview call calculates the permissions "
                         + "that would change and what their new values would be, but it does not "
                         + "actually change those permissions as replacePathPrefix does.\n\n"
                         + ""
                         + "The input parameters are passed in the payload of this request.  "
                         + "The response indicates the number of changed permission "
                         + "specifications.\n\n"
                         + ""
                         + "A valid tenant and user must be specified in the request body.",
             tags = "role",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqReplacePathPrefix.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Path prefixes replaced.",
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
     public Response replacePathPrefix(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                       InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "replacePathPrefix", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
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
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
             
         // Fill in the parameter fields.
         String tenant = payload.tenant;
         String user = payload.user;        
         String schema = payload.schema;
         String roleName = payload.roleName;
         String oldSystemId = payload.oldSystemId;
         String newSystemId = payload.newSystemId;
         String oldPrefix = payload.oldPrefix;
         String newPrefix = payload.newPrefix;
         
         // Canonicalize blank prefix values.
         if (StringUtils.isBlank(oldPrefix)) oldPrefix = "";
         if (StringUtils.isBlank(newPrefix)) newPrefix = "";
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Calculate the permissions that need to change and apply changes.
         int rows = 0;
         try {
                 rows = getRoleImpl().replacePathPrefix(schema, roleName, 
                                                        oldSystemId, newSystemId, 
                                                        oldPrefix, newPrefix, 
                                                        tenant);
             }
             catch (Exception e) {
                 String msg = MsgUtils.getMsg("SK_PERM_UPDATE_FAILED", schema, roleName,
                                              oldSystemId, oldPrefix, newSystemId, newPrefix,
                                              tenant, e.getMessage());
                 _log.error(msg);
                 return Response.status(Status.BAD_REQUEST).
                         entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         
         // ---------------------------- Success ------------------------------- 
         // Success means we updated zero or more permissions. 
         ResultChangeCount count = new ResultChangeCount();
         count.changes = rows;
         RespChangeCount r = new RespChangeCount(count);
         String s = oldSystemId + ":" + oldPrefix;
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("TAPIS_UPDATED", "Permission", s), prettyPrint, r)).build();
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

             tags = "role",
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
         if (user.length() > RoleImpl.MAX_USER_NAME_LEN) {
             String msg = MsgUtils.getMsg("SK_USER_NAME_LEN", "anyTenant", 
                                          user, RoleImpl.MAX_USER_NAME_LEN);
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

}

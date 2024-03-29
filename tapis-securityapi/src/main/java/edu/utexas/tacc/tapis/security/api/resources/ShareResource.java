package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.List;

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

import edu.utexas.tacc.tapis.security.api.requestBody.ReqShareResource;
import edu.utexas.tacc.tapis.security.api.responses.RespShare;
import edu.utexas.tacc.tapis.security.api.responses.RespShareList;
import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.security.api.utils.SKCheckAuthz;
import edu.utexas.tacc.tapis.security.authz.model.SkShare;
import edu.utexas.tacc.tapis.security.authz.model.SkShareDeleteSelector;
import edu.utexas.tacc.tapis.security.authz.model.SkShareInputFilter;
import edu.utexas.tacc.tapis.security.authz.model.SkShareList;
import edu.utexas.tacc.tapis.security.authz.model.SkSharePrivilegeSelector;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBoolean;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultBoolean;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@Path("/share")
public class ShareResource 
  extends AbstractResource
{
   /* **************************************************************************** */
   /*                                   Constants                                  */
   /* **************************************************************************** */
   // Local logger.
   private static final Logger _log = LoggerFactory.getLogger(ShareResource.class);
   
   // Json schema resource files.
   private static final String FILE_SK_SHARE_RESOURCE_REQUEST = 
       "/edu/utexas/tacc/tapis/security/api/jsonschema/ShareResourceRequest.json";
   
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
    /* shareResource:                                                               */
    /* ---------------------------------------------------------------------------- */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Share a Tapis resource using a request body.  "
                          + "Shared resources allow services to indicate that other services "
                          + "should relax their Tapis authorization checking in certain, "
                          + "well-defined contexts.\n\n"
                          + ""
                          + "Grantees can be given shared access "
                          + "to a resource on an individual basis or by "
                          + "using the public granting mechanism. Grants to the "
                          + "distinguished **~public** and **~public_no_authn** "
                          + "pseudo-grantees allow access to a resource to authenticated "
                          + "users or to any user, respectively.\n\n"
                          + ""
                          + "The payload for this request includes these values, with all "
                          + "except *resourceId2* required:\n\n"
                          + ""
                          + "   - grantor\n"
                          + "   - grantee\n"
                          + "   - tenant\n"
                          + "   - resourceType\n"
                          + "   - resourceId1\n"
                          + "   - resourceId2\n"
                          + "   - privilege\n\n"
                          + ""
                          + "If the share already exists, then this call has no effect. "
                          + "For the request to be authorized, the requestor must be "
                          + "a Tapis service."
                          + "",
            tags = "share",
            security = {@SecurityRequirement(name = "TapisJWT")},
            requestBody = 
                @RequestBody(
                    required = true,
                    content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqShareResource.class))),
            responses = 
                {@ApiResponse(responseCode = "200", description = "Share existed.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl.class))),
                 @ApiResponse(responseCode = "201", description = "Share created.",
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
    public Response shareResource(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                  InputStream payloadStream)
    {
        // Trace this request.
        if (_log.isTraceEnabled()) {
            String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                         "shareResource", _request.getRequestURL());
            _log.trace(msg);
        }
        
        // ------------------------- Input Processing -------------------------
        // Parse and validate the json in the request payload, which must exist.
        ReqShareResource payload = null;
        try {payload = getPayload(payloadStream, FILE_SK_SHARE_RESOURCE_REQUEST, 
                                  ReqShareResource.class);
        } 
        catch (Exception e) {
            String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                         "shareResource", e.getMessage());
            _log.error(msg, e);
            return Response.status(Status.BAD_REQUEST).
              entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
            
        // Fill in the parameter fields.
        var skShare = new SkShare();
        skShare.setGrantor(payload.grantor);
        skShare.setGrantee(payload.grantee);
        skShare.setTenant(payload.tenant);
        skShare.setResourceType(payload.resourceType);
        skShare.setResourceId1(payload.resourceId1);
        skShare.setResourceId2(payload.resourceId2);
        skShare.setPrivilege(payload.privilege);
        
        // The threadlocal object has not been validated yet, but it's never null.
        // Note that the obo values are null on a user token. This isn't a problem
        // because the first authz check detects non-service tokens before the
        // obo tenant and user are referenced.  JWTValidateRequestFilter guarantees 
        // that service tokens always have both obo values assigned. 
        var threadContext = TapisThreadLocal.tapisThreadContext.get();
        skShare.setCreatedBy(threadContext.getJwtUser());
        skShare.setCreatedByTenant(threadContext.getJwtTenantId());
        var oboUser   = threadContext.getOboUser();
        var oboTenant = threadContext.getOboTenantId();
        
        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(oboTenant, oboUser)
                            .setCheckIsService()
                            .check(prettyPrint);
        if (resp != null) return resp;
        
        // ------------------------ Request Processing ------------------------
        // Create the share in the database.  The share object's id, created, 
        // createdBy and createdByTenant are updated by the called code.  This
        // includes cases where the share existed or not.
        int rows = 0;
        try {rows = getShareImpl().shareResource(skShare);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_CREATE_ERROR", skShare.getGrantor(), skShare.getTenant(),
                            skShare.getGrantee(), skShare.getResourceType(), skShare.printResource(), 
                            skShare.getPrivilege());
            return getExceptionResponse(e, msg, prettyPrint);
        }
        
        // NOTE: We need to assign a location header as well.
        //       See https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5.
        ResultResourceUrl respUrl = new ResultResourceUrl();
        respUrl.url = SKApiUtils.constructTenantURL(skShare.getCreatedByTenant(), _request.getRequestURI(), 
                                                    Integer.toString(skShare.getId()));
        RespResourceUrl r = new RespResourceUrl(respUrl);
        
        // ---------------------------- Success ------------------------------- 
        // No new rows means the role exists. 
        if (rows == 0)
            return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                MsgUtils.getMsg("TAPIS_EXISTED", "Share", skShare.getId()), prettyPrint, r)).build();
        else 
            return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
                MsgUtils.getMsg("TAPIS_CREATED", "Share", skShare.getId()), prettyPrint, r)).build();
    }

    /* ---------------------------------------------------------------------------- */
    /* getShares:                                                                   */
    /* ---------------------------------------------------------------------------- */
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Get a filtered list of shared resources. "
                          + "Query parameters are used to restrict the returned shares. "
                          + "The *grantor*, *grantee*, *tenant*, *resourceType*, *resourceId1*, "
                          + "*resourceId2*, *privilege*, *createdBy* and *createdByTenant* "
                          + "parameters are used to match values in shared resource objects. "
                          + "Other query parameters are used to control how matching is "
                          + "performed.  The *tenant* parameter is required.\n\n"
                          + ""
                          + "If resourceId1 or resourceId2 end with a percent sign (%) "
                          + "wildcard then the search results will include all shares with "
                          + "IDs that begin with the same prefix string.  Percent signs "
                          + "embedded elsewhere in the string are *not* recognized as wildcards.\n\n"
                          + ""
                          + "Specifying the *id* parameter causes the other filtering "
                          + "parameters to be ignored. The result list will contain at "
                          + "most one entry.\n\n"
                          + ""
                          + "The *includePublicGrantees* flag, true by default, controls "
                          + "whether resources granted to **~public** and **~public_no_authn** "
                          + "are also considered for inclusion in the result list.\n\n"
                          + ""
                          + "The *requireNullId2* flag, true by default, applies only when no "
                          + "*resourceId2* value is provided. When set, only shared resources "
                          + "that do not specify a *resourceId2* value are considered for "
                          + "inclusion in the result list. By setting this flag to false the caller "
                          + "indicates a \"don't care\" designation on the *resourceId2* value, "
                          + "allowing shares with any *resourceId2* value to be considered for "
                          + "inclusion in the result list.\n\n"
                          + ""
                          + "For the request to be authorized, the requestor must be "
                          + "a Tapis service."
                          + "",
            tags = "share",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {@ApiResponse(responseCode = "200", description = "List of shares returned.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.security.api.responses.RespShareList.class))),
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
    public Response getShares(@DefaultValue("") @QueryParam("grantor")         String grantor,
                              @DefaultValue("") @QueryParam("grantee")         String grantee,
                              @DefaultValue("") @QueryParam("tenant")          String tenant,  // required
                              @DefaultValue("") @QueryParam("resourceType")    String resourceType,
                              @DefaultValue("") @QueryParam("resourceId1")     String resourceId1,
                              @DefaultValue("") @QueryParam("resourceId2")     String resourceId2,
                              @DefaultValue("") @QueryParam("privilege")       String privilege,
                              @DefaultValue("") @QueryParam("createdBy")       String createdBy,
                              @DefaultValue("") @QueryParam("createdByTenant") String createdByTenant,
                              @DefaultValue("true")  @QueryParam("includePublicGrantees") boolean includePublicGrantees,
                              @DefaultValue("true")  @QueryParam("requireNullId2")        boolean requireNullId2,
                              @DefaultValue("0")     @QueryParam("id")         int id,
                              @DefaultValue("false") @QueryParam("pretty")     boolean prettyPrint)
    {
        // Trace this request.
        if (_log.isTraceEnabled()) {
            String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                         "getShares", _request.getRequestURL());
            _log.trace(msg);
        }
        
        // ------------------------- Input Processing -------------------------
        // Get obo information.
        var threadContext = TapisThreadLocal.tapisThreadContext.get();
        var oboTenant = threadContext.getOboTenantId();
        var oboUser   = threadContext.getOboUser();
        
        // Convert empty strings to null and sanitize input.
        var inputFilter = new SkShareInputFilter();
        inputFilter.setGrantor(StringUtils.stripToNull(grantor));
        inputFilter.setGrantee(StringUtils.stripToNull(grantee));
        inputFilter.setTenant(StringUtils.stripToNull(tenant));  // checked for null below
        inputFilter.setResourceType(StringUtils.stripToNull(resourceType));
        inputFilter.setResourceId1(StringUtils.stripToNull(resourceId1));
        inputFilter.setResourceId2(StringUtils.stripToNull(resourceId2));
        inputFilter.setPrivilege(StringUtils.stripToNull(privilege));
        inputFilter.setCreatedBy(StringUtils.stripToNull(createdBy));
        inputFilter.setCreatedByTenant(StringUtils.stripToNull(createdByTenant));
        inputFilter.setIncludePublicGrantees(includePublicGrantees);
        inputFilter.setRequireNullId2(requireNullId2);
        inputFilter.setId(id);

        // We don't allow cross tenant queries.
        if (inputFilter.getTenant() == null) {
            var r = new RespBasic("Missing input parameter: tenant");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "tenant"), prettyPrint, r)).build();
        }
        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(oboTenant, oboUser)
                            .setCheckIsService()
                            .check(prettyPrint);
        if (resp != null) return resp;
        
        // ------------------------ Request Processing ------------------------
        // Retrieve the shared resource objects that meet the filter criteria.
        // A non-null list is always returned unless there's an exception.
        List<SkShare> list = null;
        try {list = getShareImpl().getShares(inputFilter);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_RETRIEVAL_ERROR", oboTenant, oboUser,
                                         threadContext.getJwtTenantId(), threadContext.getJwtUser(),
                                         inputFilter.getTenant());
            return getExceptionResponse(e, msg, prettyPrint);
        }
        
        // Package the list for the response.
        var skShares = new SkShareList();
        skShares.shares = list;
        
        // ---------------------------- Success ------------------------------- 
        // Success means zero or more shares were found. 
        RespShareList r = new RespShareList(skShares);
        return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            MsgUtils.getMsg("TAPIS_FOUND", "Shares", skShares.shares.size()), prettyPrint, r)).build();
    }

    /* ---------------------------------------------------------------------------- */
    /* getShare:                                                                    */
    /* ---------------------------------------------------------------------------- */
    @GET
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Get a shared resource by ID. "
                          + "The shared resource is deleted only if it's in the tenant "
                          + "specified in the required *tenant* query parameter.\n\n"
                          + ""
                          + "For the request to be authorized, the requestor must be "
                          + "a Tapis service."
                          + "",
            tags = "share",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {@ApiResponse(responseCode = "200", description = "A share returned.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.security.api.responses.RespShare.class))),
                 @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "404", description = "Not found.",
                 content = @Content(schema = @Schema(
                    implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
        )
    public Response getShare(@PathParam("id") int id,
                             @DefaultValue("") @QueryParam("tenant") String tenant,  // required
                             @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
    {
        // Trace this request.
        if (_log.isTraceEnabled()) {
            String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                         "getShares", _request.getRequestURL());
            _log.trace(msg);
        }
        
        // ------------------------- Input Processing -------------------------
        // The id must be greater than zero.
        if (id <= 0) {
            var r = new RespBasic("Invalid share id: " + id + ".");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "Share", id), prettyPrint, r)).build();
        }
        
        // Make sure we have an actual tenant string.
        tenant = StringUtils.stripToNull(tenant);
        if (tenant == null) {
            var r = new RespBasic("Missing tenant query parameter.");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "Share", "tenant"), prettyPrint, r)).build();
        }
        
        // Get obo information.
        var threadContext = TapisThreadLocal.tapisThreadContext.get();
        var oboTenant = threadContext.getOboTenantId();
        var oboUser   = threadContext.getOboUser();

        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(oboTenant, oboUser)
                            .setCheckIsService()
                            .check(prettyPrint);
        if (resp != null) return resp;
        
        // ------------------------ Request Processing ------------------------
        // Retrieve the shared resource objects that meet the filter criteria.
        // A non-null list is always returned unless there's an exception.
        SkShare skShare = null;
        try {skShare = getShareImpl().getShare(tenant, id);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_RETRIEVAL_ERROR", oboTenant, oboUser,
                                         threadContext.getJwtTenantId(), threadContext.getJwtUser(),
                                         tenant);
            return getExceptionResponse(e, msg, prettyPrint);
        }
        
        // Surface not found as an error.
        if (skShare == null) {
            var r = new RespBasic("No share with id " + id + " was found.");
            return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "Share", id), prettyPrint, r)).build();
        }
                
        // ---------------------------- Success ------------------------------- 
        // Success means zero or more shares were found. 
        RespShare r = new RespShare(skShare);
        return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            MsgUtils.getMsg("TAPIS_FOUND", "Shares", id), prettyPrint, r)).build();
    }

    /* ---------------------------------------------------------------------------- */
    /* deleteShareById:                                                             */
    /* ---------------------------------------------------------------------------- */
    @DELETE
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Delete a shared resource by ID. "
                          + "The shared resource is deleted only if it's in the tenant "
                          + "specified in the required *tenant* query parameter. The "
                          + "calling service must also be the same as the orginal "
                          + "service that created the share.\n\n"
                          + ""
                          + "This call is idempotent.  If no share satisfies the above "
                          + "constraints, a success response code is returned and the "
                          + "indicated number of changes is set to zero.  When a share "
                          + "is deleted, the indicated number of changes is one.\n\n"
                          + ""
                          + "For the request to be authorized, the requestor must be "
                          + "the Tapis service that originally granted the share."
                          + "",
            tags = "share",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {@ApiResponse(responseCode = "200", description = "A share deleted if it exists.",
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
    public Response deleteShareById(@PathParam("id") int id,
                                    @DefaultValue("") @QueryParam("tenant") String tenant,  // required
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
    {
        // Trace this request.
        if (_log.isTraceEnabled()) {
            String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                         "deleteSharesById", _request.getRequestURL());
            _log.trace(msg);
        }
        
        // ------------------------- Input Processing -------------------------
        // The id must be greater than zero.
        if (id <= 0) {
            var r = new RespBasic("Invalid share id: " + id + ".");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "deleteShare", id), prettyPrint, r)).build();
        }
        
        // Make sure we have an actual tenant string.
        tenant = StringUtils.stripToNull(tenant);
        if (tenant == null) {
            var r = new RespBasic("Missing tenant query parameter.");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "Share", "tenant"), prettyPrint, r)).build();
        }
        
        // Get obo information.
        var threadContext = TapisThreadLocal.tapisThreadContext.get();
        var oboTenant = threadContext.getOboTenantId();
        var oboUser   = threadContext.getOboUser();
        var jwtTenant = threadContext.getJwtTenantId();
        var jwtUser   = threadContext.getJwtUser();

        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(oboTenant, oboUser)
                            .setCheckIsService()
                            .check(prettyPrint);
        if (resp != null) return resp;
        
        // ------------------------ Request Processing ------------------------
        // Retrieve the shared resource objects that meet the filter criteria.
        // A non-null list is always returned unless there's an exception.
        int rows = 0;
        try {rows = getShareImpl().deleteShare(tenant, id, jwtTenant, jwtUser);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_DELETE_BY_ID_ERROR", oboTenant, oboUser,
                                         jwtTenant, jwtUser, id, tenant);
            return getExceptionResponse(e, msg, prettyPrint);
        }
        
        // Package the count.
        var resultCount = new ResultChangeCount();
        resultCount.changes = rows;
        var r = new RespChangeCount(resultCount);
        
        // This call is idempotent but returns a different response message when ID not found.
        if (rows < 1) {
            return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "deleteShare", id), prettyPrint, r)).build();
        }
                
        // ---------------------------- Success ------------------------------- 
        // Success means zero or more shares were found. 
        return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            MsgUtils.getMsg("TAPIS_FOUND", "deleteShare", id), prettyPrint, r)).build();
    }

    /* ---------------------------------------------------------------------------- */
    /* deleteShare:                                                                 */
    /* ---------------------------------------------------------------------------- */
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Delete a single shared resource by unique attribute selection. "
                          + "The *grantor*, *grantee*, *tenant*, *resourceType*, "
                          + "*resourceId1* and *privilege* parameters are mandatory; "
                          + "*resourceId2* is optional and assumed to be NULL if not "
                          + "provided.\n\n"
                          + ""
                          + "The shared resource is deleted only if it's in the tenant "
                          + "specified in the required *tenant* query parameter. The "
                          + "calling service must also be the same as the orginal service "
                          + "that granted the share.\n\n"
                          + ""
                          + "This call is idempotent.  If no share satisfies the above "
                          + "constraints, a success response code is returned and the "
                          + "indicated number of changes is set to zero.  When a share "
                          + "is deleted, the indicated number of changes is one.\n\n"
                          + ""
                          + "For the request to be authorized, the requestor must be "
                          + "the Tapis service that originally granted the share."
                          + "",
            tags = "share",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {@ApiResponse(responseCode = "200", description = "A share deleted if it exists.",
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
    public Response deleteShare(@DefaultValue("") @QueryParam("grantor")      String grantor,
                                @DefaultValue("") @QueryParam("grantee")      String grantee,
                                @DefaultValue("") @QueryParam("tenant")       String tenant,
                                @DefaultValue("") @QueryParam("resourceType") String resourceType,
                                @DefaultValue("") @QueryParam("resourceId1")  String resourceId1,
                                @DefaultValue("") @QueryParam("resourceId2")  String resourceId2,
                                @DefaultValue("") @QueryParam("privilege")    String privilege,
                                @DefaultValue("false") @QueryParam("pretty")  boolean prettyPrint)
    {
        // Trace this request.
        if (_log.isTraceEnabled()) {
            String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                         "deleteShares", _request.getRequestURL());
            _log.trace(msg);
        }
        
        // ------------------------- Input Processing -------------------------        
        // Get obo information.
        var threadContext = TapisThreadLocal.tapisThreadContext.get();
        var oboTenant = threadContext.getOboTenantId();
        var oboUser   = threadContext.getOboUser();
        var jwtTenant = threadContext.getJwtTenantId();
        var jwtUser   = threadContext.getJwtUser();

        // Package input parameters. 
        var sel = new SkShareDeleteSelector();
        sel.setGrantor(StringUtils.stripToNull(grantor));
        sel.setGrantee(StringUtils.stripToNull(grantee));
        sel.setTenant(StringUtils.stripToNull(tenant));
        sel.setResourceType(StringUtils.stripToNull(resourceType));
        sel.setResourceId1(StringUtils.stripToNull(resourceId1));
        sel.setResourceId2(StringUtils.stripToNull(resourceId2)); 
        sel.setPrivilege(StringUtils.stripToNull(privilege));
        sel.setCreatedBy(jwtUser);
        sel.setCreatedByTenant(jwtTenant);
        
        // Validate inputs. Only id2 can be null.
        if (sel.getGrantor() == null) {
            var r = new RespBasic("Missing input parameter: grantor");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "grantor"), prettyPrint, r)).build();
        }
        if (sel.getGrantee() == null) {
            var r = new RespBasic("Missing input parameter: grantee");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "grantee"), prettyPrint, r)).build();
        }
        if (sel.getTenant() == null) {
            var r = new RespBasic("Missing input parameter: tenant");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "tenant"), prettyPrint, r)).build();
        }
        if (sel.getResourceType() == null) {
            var r = new RespBasic("Missing input parameter: resourceType");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "resourceType"), prettyPrint, r)).build();
        }
        if (sel.getResourceId1() == null) {
            var r = new RespBasic("Missing input parameter: resourceId1");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "resourceId1"), prettyPrint, r)).build();
        }
        if (sel.getPrivilege() == null) {
            var r = new RespBasic("Missing input parameter: privilege");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "privilege"), prettyPrint, r)).build();
        }
        
        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(oboTenant, oboUser)
                            .setCheckIsService()
                            .check(prettyPrint);
        if (resp != null) return resp;
        
        // ------------------------ Request Processing ------------------------
        // Retrieve the shared resource objects that meet the filter criteria.
        // A non-null list is always returned unless there's an exception.
        int rows = 0;
        try {rows = getShareImpl().deleteShare(sel);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_DELETE_ERROR", oboTenant, oboUser,
                                         jwtTenant, jwtUser, grantee, grantor, tenant);
            return getExceptionResponse(e, msg, prettyPrint);
        }
        
        // Package the count.
        var resultCount = new ResultChangeCount();
        resultCount.changes = rows;
        var r = new RespChangeCount(resultCount);
        
        // This call is idempotent but returns a different response message when ID not found.
        if (rows < 1) {
            return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "deleteShare", grantee), prettyPrint, r)).build();
        }
                
        // ---------------------------- Success ------------------------------- 
        // Success means zero or more shares were found. 
        return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            MsgUtils.getMsg("TAPIS_FOUND", "deleteShare", grantee), prettyPrint, r)).build();
    }

    /* ---------------------------------------------------------------------------- */
    /* hasPrivilege:                                                                */
    /* ---------------------------------------------------------------------------- */
    @GET
    @Path("/hasPrivilege")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Determine if a user has been granted a specific privilege "
                          + "on a specific resource. The *grantee*, *tenant*, *resourceType*, "
                          + "*resourceId1* and *privilege* parameters are mandatory; "
                          + "*resourceId2* is optional and assumed to be NULL if not "
                          + "provided. Privilege matching is performed for the grantee "
                          + "and tenant specified in the query parameters.\n\n"
                          + ""
                          + "True is returned if the user has been granted the privilege, "
                          + "false otherwise.\n\n"
                          + ""
                          + "By default, both authenticated and unauthenticated "
                          + "public privileges are included in the calculation. For "
                          + "example, if a privilege on a resource has been granted "
                          + "to all authenticated users in a tenant (~public), "
                          + "then true will be returned for all users in the tenant.\n\n"
                          + ""
                          + "The *excludePublic* and *excludePublicNoAuthn* parameters "
                          + "can be used to change the default handling of public "
                          + "grants. Either or both types of public grants can "
                          + "be excluded.\n\n"
                          + ""
                          + "For the request to be authorized, the requestor must be "
                          + "a Tapis service."
                          + "",
            tags = "share",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {@ApiResponse(responseCode = "200", description = "A share returned.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBoolean.class))),
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
    public Response hasPrivilege(@DefaultValue("") @QueryParam("grantee") String grantee,
                                 @DefaultValue("") @QueryParam("tenant") String tenant,
                                 @DefaultValue("") @QueryParam("resourceType") String resourceType,
                                 @DefaultValue("") @QueryParam("resourceId1")  String resourceId1,
                                 @DefaultValue("") @QueryParam("resourceId2")  String resourceId2,
                                 @DefaultValue("") @QueryParam("privilege")    String privilege,
                                 @DefaultValue("false") @QueryParam("excludePublic") boolean excludePublic,
                                 @DefaultValue("false") @QueryParam("excludePublicNoAuthn") boolean excludePublicNoAuthn,
                                 @DefaultValue("false") @QueryParam("pretty")  boolean prettyPrint)
    {
        // Trace this request.
        if (_log.isTraceEnabled()) {
            String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                         "hasPrivilege", _request.getRequestURL());
            _log.trace(msg);
        }
        
        // ------------------------- Input Processing -------------------------        
        // Get obo information.
        var threadContext = TapisThreadLocal.tapisThreadContext.get();
        var oboTenant = threadContext.getOboTenantId();
        var oboUser   = threadContext.getOboUser();

        // Package input parameters. 
        var sel = new SkSharePrivilegeSelector();
        sel.setGrantee(StringUtils.stripToNull(grantee));
        sel.setTenant(StringUtils.stripToNull(tenant));
        sel.setResourceType(StringUtils.stripToNull(resourceType));
        sel.setResourceId1(StringUtils.stripToNull(resourceId1));
        sel.setResourceId2(StringUtils.stripToNull(resourceId2)); 
        sel.setPrivilege(StringUtils.stripToNull(privilege));
        
        // Validate inputs. Only id2 can be null.
        if (sel.getGrantee() == null) {
            var r = new RespBasic("Missing input parameter: grantee");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "grantee"), prettyPrint, r)).build();
        }
        if (sel.getTenant() == null) {
            var r = new RespBasic("Missing input parameter: tenant");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "tenant"), prettyPrint, r)).build();
        }
        if (sel.getResourceType() == null) {
            var r = new RespBasic("Missing input parameter: resourceType");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "resourceType"), prettyPrint, r)).build();
        }
        if (sel.getResourceId1() == null) {
            var r = new RespBasic("Missing input parameter: resourceId1");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "resourceId1"), prettyPrint, r)).build();
        }
        if (sel.getPrivilege() == null) {
            var r = new RespBasic("Missing input parameter: privilege");
            return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", "privilege"), prettyPrint, r)).build();
        }
        
        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(oboTenant, oboUser)
                            .setCheckIsService()
                            .check(prettyPrint);
        if (resp != null) return resp;
        
        // ------------------------ Request Processing ------------------------
        // Retrieve the shared resource objects that meet the filter criteria.
        // A non-null list is always returned unless there's an exception.
        boolean hasPrivilege = false;
        try {hasPrivilege = getShareImpl().hasPrivilege(sel);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_RETRIEVAL_ERROR", oboTenant, oboUser,
                                         threadContext.getJwtTenantId(), threadContext.getJwtUser(),
                                         sel.getTenant());
            return getExceptionResponse(e, msg, prettyPrint);
        }
        
        // Create the response.
        var resultBoolean = new ResultBoolean();
        resultBoolean.aBool = hasPrivilege;
        var r = new RespBoolean(resultBoolean);
        
        // Surface not found as an error.
        if (!hasPrivilege) {
            return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                    MsgUtils.getMsg("TAPIS_NOT_FOUND", "hasPrivilege", sel.getPrivilege()), prettyPrint, r)).build();
        }
                
        // ---------------------------- Success ------------------------------- 
        // Success means zero or more shares were found. 
        return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            MsgUtils.getMsg("TAPIS_FOUND", "hasPrivilege", sel.getPrivilege()), prettyPrint, r)).build();
    }
}

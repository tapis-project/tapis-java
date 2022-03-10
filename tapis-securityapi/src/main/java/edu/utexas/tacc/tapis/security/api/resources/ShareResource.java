package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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
import edu.utexas.tacc.tapis.security.api.responses.RespShareList;
import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.security.api.utils.SKCheckAuthz;
import edu.utexas.tacc.tapis.security.authz.dao.SkShareDao.ShareFilter;
import edu.utexas.tacc.tapis.security.authz.model.SkShare;
import edu.utexas.tacc.tapis.security.authz.model.SkShareList;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
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
                          + "Shared resources allow services to tell other services "
                          + "to relax their Tapis authorization checking in certain "
                          + "contexts where a grantee has been given shared access. "
                          + "The distinguished ~public and ~public_no_authn grantees "
                          + "are used to grant different types of public access to a "
                          + "resource.\n\n"
                          + ""
                          + "The payload for this includes these values, with all "
                          + "except resourceId2 required:\n\n"
                          + ""
                          + "   - grantee\n"
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
        skShare.setGrantee(payload.grantee);
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
        skShare.setGrantor(threadContext.getOboUser());
        skShare.setTenant(threadContext.getOboTenantId());
        skShare.setCreatedBy(threadContext.getJwtUser());
        skShare.setCreatedByTenant(threadContext.getJwtTenantId());
        
        // ------------------------- Check Authz ------------------------------
        // Authorization passed if a null response is returned.
        Response resp = SKCheckAuthz.configure(skShare.getTenant(), skShare.getGrantor())
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
                MsgUtils.getMsg("TAPIS_EXISTED", "Share", skShare.printResource()), prettyPrint, r)).build();
        else 
            return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
                MsgUtils.getMsg("TAPIS_CREATED", "Share", skShare.printResource()), prettyPrint, r)).build();
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
                          + "The *grantor*, *grantee*, *resourceType*, *resourceId1*, "
                          + "*resourceId2*, *privilege*, *createdBy* and *createdByTenant* "
                          + "parameters are used to match values in shared resource objects. "
                          + "The other parameters are used to control how matching is "
                          + "performed.\n\n"
                          + ""
                          + "Specifying the *id* parameter causes the other filtering "
                          + "parameters to be ignored. The result list will contain at "
                          + "most one entry.\n\n"
                          + ""
                          + "The *includePublicGrantees* flag, true by default, controls "
                          + "whether resources granted to **~public** and **~public_no_authn** "
                          + "that meet all other constraints are included in the results.\n\n"
                          + ""
                          + "The *requireNullId2* flag, true by default, applies only when no "
                          + "*resourceId2* value is provided. By default, only resources granted "
                          + "with no *resourceId2* value and meet all other constraints "
                          + "are included in the results. By setting this flag to false the caller "
                          + "indicates \"don't care\" designation on the *resourceId2* values, "
                          + "allowing shares with any *resourceId2* value that meet all other "
                          + "constraints to be included in the results."
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
        // Convert empty strings to null and sanitize input.
        grantor         = StringUtils.stripToNull(grantor);
        grantee         = StringUtils.stripToNull(grantee);
        resourceType    = StringUtils.stripToNull(resourceType);
        resourceId1     = StringUtils.stripToNull(resourceId1);
        resourceId2     = StringUtils.stripToNull(resourceId2);
        privilege       = StringUtils.stripToNull(privilege);
        createdBy       = StringUtils.stripToNull(createdBy);
        createdByTenant = StringUtils.stripToNull(createdByTenant);
        
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
        
        // ------------------------ Parameter Set Up --------------------------
        // Populate the query map which always contains the obo tenant.
        var filter = new HashMap<ShareFilter,Object>();
        filter.put(ShareFilter.TENANT, oboTenant);
        
        // Optional query parameters.
        if (grantor != null) filter.put(ShareFilter.GRANTOR, grantor);
        if (grantee != null) filter.put(ShareFilter.GRANTEE, grantee);
        if (resourceType != null) filter.put(ShareFilter.RESOURCE_TYPE, resourceType);
        if (resourceId1 != null) filter.put(ShareFilter.RESOURCE_ID1, resourceId1);
        if (resourceId2 != null) filter.put(ShareFilter.RESOURCE_ID2, resourceId2);
        if (privilege != null) filter.put(ShareFilter.PRIVILEGE, privilege);
        if (createdBy != null) filter.put(ShareFilter.CREATEDBY, createdBy);
        if (createdByTenant != null) filter.put(ShareFilter.CREATEDBY_TENANT, createdByTenant);
        
        // These query parameters are always non-null.
        filter.put(ShareFilter.INCLUDE_PUBLIC_GRANTEES, includePublicGrantees);
        filter.put(ShareFilter.REQUIRE_NULL_ID2, requireNullId2);
        if (id > 0) filter.put(ShareFilter.ID, id); // valid seqno's only
        
        // ------------------------ Request Processing ------------------------
        // Retrieve the shared resource objects that meet the filter criteria.
        // A non-null list is always returned unless there's an exception.
        List<SkShare> list = null;
        try {list = getShareImpl().getShares(filter);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_SHARE_RETRIEVAL_ERROR", oboTenant, oboUser,
                                         threadContext.getJwtTenantId(), threadContext.getJwtUser());
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
}

package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.requestBody.ReqShareResource;
import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.security.api.utils.SKCheckAuthz;
import edu.utexas.tacc.tapis.security.authz.model.SkShare;
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
    /* getRoleNames:                                                                */
    /* ---------------------------------------------------------------------------- */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Share a Tapis resource using a request body.  "
                          + "Role names are case sensitive, alpha-numeric "
                          + "strings that can also contain underscores.  Role names must "
                          + "start with an alphbetic character and can be no more than 58 "
                          + "characters in length.  The desciption can be no more than "
                          + "2048 characters long.  If the role already exists, this "
                          + "request has no effect.\n\n"
                          + ""
                          + "For the request to be authorized, the requestor must be "
                          + "either an administrator or a service allowed to perform "
                          + "updates in the new role's tenant."
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
        // Create the share in the database.  The share object's id and timestamp
        // are updated by the called code.
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
}

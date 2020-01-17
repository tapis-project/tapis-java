package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.HashMap;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bettercloud.vault.response.LogicalResponse;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.security.api.requestBody.ReqWriteSecret;
import edu.utexas.tacc.tapis.security.api.responses.RespSecret;
import edu.utexas.tacc.tapis.security.secrets.VaultManager;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/vault")
public final class VaultResource
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(VaultResource.class);

    // Json schema resource files.
    private static final String FILE_SK_WRITE_SECRET_REQUEST = 
        "/edu/utexas/tacc/tapis/security/api/jsonschema/WriteSecretRequest.json";
    
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
     /* readSecret:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/secret/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Read a versioned secret. "
                           + "A secret is read from a path name constructed using the "
                           + "secretName URL path parameter. The path name also includes the tenant "
                           + "and user who owns the secret.\n\n"
                           + ""
                           + "By default, the "
                           + "latest version of the secret is read. If the 'version' query parameter "
                           + "is specified then that version of the secret is read if it exists. "
                           + "The 'version' parameter should be passed as an integer. Zero "
                           + "indicates that the latest version of the secret should be "
                           + "returned. A NOT FOUND status code is returned if the secret does not "
                           + "exist.\n\n"
                           + ""
                           + "The response object includes the map of zero or more key/value "
                           + "pairs and metadata that describes the secret, including which version of "
                           + "the secret was returned.",
             tags = "vault",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret written.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespSecret.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Not found.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response readSecret(@PathParam("secretName") String secretName,
                                @DefaultValue("0") @QueryParam("version") int version,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "readSecret", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Construct the secret's full path that include tenant and user.
         String secretPath = getSecretPath(threadContext, secretName);
         
         // Issue the vault call.
         LogicalResponse logicalResp = null;
         try {
             var logical = VaultManager.getInstance().getVault().logical();
             logicalResp = logical.read(secretPath, Boolean.TRUE, version);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          secretPath, version, e.getMessage());
             return getExceptionResponse(e, msg, prettyPrint);
         }
         
         // The rest response field is non-null if we get here.
         var restResp = logicalResp.getRestResponse();
         int vaultStatus = restResp.getStatus();
         String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
         
         // Did vault encounter a problem?
         if (vaultStatus >= 400) {
             String msg = MsgUtils.getMsg("SK_VAULT_READ_SECRET_ERROR", 
                     threadContext.getTenantId(), threadContext.getUser(), 
                     secretPath, version, vaultBody);
             _log.error(msg);
             return Response.status(vaultStatus).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------ Request Output ----------------------------
         // Create the response object.
         var respSecret = new RespSecret();
         
         // Get the outer data contains everything we're interested in.
         var bodyJson = TapisGsonUtils.getGson().fromJson(vaultBody, JsonObject.class);
         JsonObject dataObj = (JsonObject) bodyJson.get("data");
         if (dataObj != null) {
             // The inner data object is a map of zero or more key/value pairs.
             JsonObject mapObj = (JsonObject) dataObj.get("data");
             if (mapObj != null) {
                 for (var entry : mapObj.entrySet()) {
                     respSecret.secretMap.put(entry.getKey(), entry.getValue().getAsString());
                 }
             }
             
             // Get the secret metadata.
             JsonObject metaObj = (JsonObject) dataObj.get("metadata");
             if (metaObj != null) 
                 respSecret.metadata = TapisGsonUtils.getGson().fromJson(metaObj, RespSecret.SecretMetadata.class); 
         }
         
         // Success.
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_READ", "Secret", secretName), prettyPrint, respSecret)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* writeSecret:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Create or update a secret. "
                           + "A secret is assigned a path name constructed using the "
                           + "secretName URL path parameter. The path name also includes the tenant "
                           + "and user on behalf of whom the the secret is being saved. At the "
                           + "top-level, the JSON payload may contain an optional 'options' object "
                           + "and a required 'data' object.\n\n"
                           + ""
                           + "The 'data' object is a JSON object that contains one or more key/value "
                           + "pairs in which both the key and value are strings. These are the "
                           + "individual secrets that are saved under the path name. The secrets are "
                           + "automatically versioned, which allows a pre-configured number of past "
                           + "secret values to be accessible even after new values are assigned. See "
                           + "the various read operations for details on how to access different "
                           + "versions of a secret.\n\n"
                           + ""
                           + "NOTE: The 'cas' option is currently ignored but documented here for "
                           + "future reference.\n\n"
                           + ""
                           + "The options object can contains a 'cas' key and with an integer "
                           + "value that represents as secret version.  CAS stands for "
                           + "check-and-set and will check an existing secret's version before "
                           + "updating.  If cas is not set the write will be always be allowed. "
                           + "If set to 0, a write will only be allowed if the key doesn’t exist. "
                           + "If the index is greater than zero the write will only be allowed if "
                           + "the key’s current version matches the version specified in the cas "
                           + "parameter.",
             tags = "vault",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqWriteSecret.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret written.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "204", description = "No content.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response writeSecret(@PathParam("secretName") String secretName,
                                 @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                 InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "writeSecret", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqWriteSecret payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_WRITE_SECRET_REQUEST, 
                                   ReqWriteSecret.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "writeSecret", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Unpack the payload.
         var secretMap = new HashMap<String,Object>();
         if (payload.data != null) secretMap.putAll(payload.data);
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Construct the secret's full path that include tenant and user.
         String secretPath = getSecretPath(threadContext, secretName);
         
         // Issue the vault call.
         LogicalResponse logicalResp = null;
         try {
             var logical = VaultManager.getInstance().getVault().logical();
             logicalResp = logical.write(secretPath, secretMap);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("SK_VAULT_WRITE_SECRET_ERROR", 
                                          threadContext.getTenantId(), threadContext.getUser(), 
                                          secretPath, e.getMessage());
             return getExceptionResponse(e, msg, prettyPrint);
         }
         
         // The rest response field is non-null if we get here.
         var restResp = logicalResp.getRestResponse();
         int vaultStatus = restResp.getStatus();
         String vaultBody = restResp.getBody() == null ? "{}" : new String(restResp.getBody());
         
         // Did vault encounter a problem?
         if (vaultStatus >= 400) {
             String msg = MsgUtils.getMsg("SK_VAULT_WRITE_SECRET_ERROR", 
                     threadContext.getTenantId(), threadContext.getUser(), 
                     secretPath, vaultBody);
             _log.error(msg);
             return Response.status(vaultStatus).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Return the data portion of the vault response.
         var bodyJson = TapisGsonUtils.getGson().fromJson(vaultBody, JsonObject.class);
         RespBasic r = new RespBasic(bodyJson.get("data"));
         return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_CREATED", "Secret", secretName), prettyPrint, r)).build();
     }
     
     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* getSecretPath:                                                               */
     /* ---------------------------------------------------------------------------- */
     /** Construct the vault v2 secret pathname.
      * 
      * @param threadContext the caller's information
      * @param secretName the user-specified secret
      * @return the full secret path
      */
     private String getSecretPath(TapisThreadContext threadContext, String secretName)
     {
         return "secret/tapis/" + threadContext.getTenantId() + "/" + threadContext.getUser() +
                "/" + secretName;
     }
}

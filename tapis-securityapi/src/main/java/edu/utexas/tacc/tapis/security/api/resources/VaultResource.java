package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.requestBody.ReqVersions;
import edu.utexas.tacc.tapis.security.api.requestBody.ReqWriteSecret;
import edu.utexas.tacc.tapis.security.api.responses.RespSecret;
import edu.utexas.tacc.tapis.security.api.responses.RespSecretList;
import edu.utexas.tacc.tapis.security.api.responses.RespSecretMeta;
import edu.utexas.tacc.tapis.security.api.responses.RespSecretVersionMetadata;
import edu.utexas.tacc.tapis.security.api.responses.RespVersions;
import edu.utexas.tacc.tapis.security.authz.model.SkSecret;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretList;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretMetadata;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretVersionMetadata;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

/** Endpoints that communicate with Hashicorp Vault.
 * 
 *  Driver enhancements:
 *      1. Add secrets v2 options object to create (write).
 *      2. Add readMeta 
 *      3. Add updateMeta
 *      4. deleteLast - soft delete of latest version 
 * 
 * @author rcardone
 */
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
    private static final String FILE_SK_SECRET_VERSION_REQUEST = 
        "/edu/utexas/tacc/tapis/security/api/jsonschema/SecretVersionRequest.json";
    
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
                           + "A secret is read from a path name constructed from the "
                           + "*secretName* parameter. The constructed path name always "
                           + "includes the tenant and user who owns the secret.\n\n"
                           + ""
                           + "By default, the "
                           + "latest version of the secret is read. If the *version* query parameter "
                           + "is specified then that version of the secret is read. "
                           + "The *version* parameter should be passed as an integer. Zero "
                           + "indicates that the latest version of the secret should be "
                           + "returned. A NOT FOUND status code is returned if the secret version "
                           + "does not exist or if it's deleted or destroyed.\n\n"
                           + ""
                           + "The response object includes the map of zero or more key/value "
                           + "pairs and metadata that describes the secret. The metadata includes "
                           + "which version of the secret was returned.",
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
         // Issue the vault call.
         SkSecret skSecret = null;
         try {
             skSecret = getVaultImpl().secretRead(threadContext.getTenantId(), threadContext.getUser(), 
                                                  secretName, version);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // ------------------------ Request Output ----------------------------
         // Create the response object.
         var respSecret = new RespSecret(skSecret);
         
         // Success.
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_READ", "Secret", secretName), prettyPrint, respSecret)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* writeSecret:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Create or update a secret. "
                           + "A secret is assigned a path name constructed from the "
                           + "*secretName* parameter. The path name also includes the tenant "
                           + "and user on behalf of whom the the secret is being saved. At the "
                           + "top-level, the JSON payload contains an optional *options* object "
                           + "and a required *data* object.\n\n"
                           + ""
                           + "The *data* object is a JSON object that contains one or more key/value "
                           + "pairs in which both the key and value are strings. These are the "
                           + "individual secrets that are saved under the path name. The secrets are "
                           + "automatically versioned, which allows a pre-configured number of past "
                           + "secret values to be accessible even after new values are assigned. See "
                           + "the various GET operations for details on how to access different "
                           + "aspects of secrets.\n\n"
                           + ""
                           + "NOTE: The *cas* option is currently ignored but documented here for "
                           + "future reference.\n\n"
                           + ""
                           + "The *options* object can contain a *cas* key and with an integer "
                           + "value that represents a secret version.  CAS stands for "
                           + "check-and-set and will check an existing secret's version before "
                           + "updating.  If cas is not set the write will be always be allowed. "
                           + "If set to 0, a write will only be allowed if the key doesn’t exist. "
                           + "If the index is greater than zero, then the write will only be allowed "
                           + "if the key’s current version matches the version specified in the cas "
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
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespSecretMeta.class))),
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
         // Note that the secret values in the payload will only be string values,
         // which is more restrictive typing than Vault.
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
         // Issue the vault call.
         SkSecretMetadata skSecretMeta = null;
         try {
             skSecretMeta = getVaultImpl().secretWrite(threadContext.getTenantId(), 
                                                       threadContext.getUser(), 
                                                       secretName, secretMap);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespSecretMeta r = new RespSecretMeta(skSecretMeta);
         return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_CREATED", "Secret", secretName), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* deleteSecret:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/delete/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Soft delete one or more versions of a secret. "
                           + "Each version can be deleted individually or as part of a "
                           + "group specified in the input array. Deletion can be "
                           + "reversed using the *secret/undelete/{secretName}* "
                           + "endpoint, which make this a _soft_ deletion operation.\n\n"
                           + ""
                           + "The input versions array is interpreted as follows:\n\n"
                           + ""
                           + "   * [ ] - empty = delete all versions\n"
                           + "   * [0] - zero = delete only the latest version\n"
                           + "   * [1, 3, ...] - list = delete the specified versions\n"
                           + "",
             tags = "vault",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqVersions.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret deleted.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespVersions.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Secret not found.",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
         )
     public Response deleteSecret(@PathParam("secretName") String secretName,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                  InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "deleteSecret", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqVersions payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_SECRET_VERSION_REQUEST, 
                                   ReqVersions.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "deleteSecret", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Massage the input.
         if (payload.versions == null) payload.versions = new ArrayList<>();
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         List<Integer> deletedVersions = null;
         try {
             deletedVersions = getVaultImpl().secretDelete(threadContext.getTenantId(), 
                                                           threadContext.getUser(), 
                                                           secretName, payload.versions);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespVersions r = new RespVersions(deletedVersions);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "Secret", secretName), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* undeleteSecret:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/undelete/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Restore one or more versions of a secret that have previously been deleted. "
                           + "This endpoint undoes soft deletions performed using the "
                           + "*secret/delete/{secretName}* endpoint.\n\n"
                           + ""
                           + "The input versions array is interpreted as follows:\n\n"
                           + ""
                           + "   * [ ] - empty = undelete all versions\n"
                           + "   * [0] - zero = undelete only the latest version\n"
                           + "   * [1, 3, ...] - list = undelete the specified versions\n"
                           + "",
             tags = "vault",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqVersions.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret written.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespVersions.class))),
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
     public Response undeleteSecret(@PathParam("secretName") String secretName,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "undeleteSecret", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // Parse and validate the json in the request payload, which must exist.
         ReqVersions payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_SECRET_VERSION_REQUEST, 
                                   ReqVersions.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "undeleteSecret", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Massage the input.
         if (payload.versions == null) payload.versions = new ArrayList<>();
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         List<Integer> undeletedVersions = null;
         try {
             undeletedVersions = getVaultImpl().secretUndelete(threadContext.getTenantId(), 
                                                               threadContext.getUser(), 
                                                               secretName, payload.versions);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespVersions r = new RespVersions(undeletedVersions);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_UNDELETED", "Secret", secretName), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* destroySecret:                                                               */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/destroy/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Destroy one or more versions of a secret. Destroy implements "
                           + "a hard delete which delete that cannot be undone. It does "
                           + "not, however, remove any metadata associated with the secret.\n\n"
                           + ""
                           + "The input versions array is interpreted as follows:\n\n"
                           + ""
                           + "   * [ ] - empty = destroy all versions\n"
                           + "   * [0] - zero = destroy only the latest version\n"
                           + "   * [1, 3, ...] - list = destroy the specified versions\n"
                           + "",
             tags = "vault",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqVersions.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret written.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespVersions.class))),
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
     public Response destroySecret(@PathParam("secretName") String secretName,
                                   @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                   InputStream payloadStream)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "destroySecret", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqVersions payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_SECRET_VERSION_REQUEST, 
                                   ReqVersions.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "destroySecret", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Massage the input.
         if (payload.versions == null) payload.versions = new ArrayList<>();
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         List<Integer> destroyedVersions = null;
         try {
             destroyedVersions = getVaultImpl().secretDestroy(threadContext.getTenantId(), 
                                                              threadContext.getUser(), 
                                                              secretName, payload.versions);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespVersions r = new RespVersions(destroyedVersions);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "Secret", secretName), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* readSecretMetadata:                                                          */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/secret/read/meta/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "List a secret's metadata including its version information. "
                         + "The input parameter must be a secret name, not a folder. "
                         + "The result includes which version of the secret is the latest."
                         + "",
             tags = "vault",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret read.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespSecretVersionMetadata.class))),
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
     public Response readSecretMeta(@PathParam("secretName") String secretName,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "readSecretMeta", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         SkSecretVersionMetadata info = null;
         try {
             info = getVaultImpl().secretReadMeta(threadContext.getTenantId(), threadContext.getUser(), 
                                                  secretName);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         var r = new RespSecretVersionMetadata(info);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_READ", "Secret", secretName), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* listSecretMeta:                                                              */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/secret/list/meta")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "List the secret names at the specified path. "
                           + "The path must represent a folder, not an actual secret name. "
                           + "If the path does not have a trailing slash one will be inserted. "
                           + "Secret names should not encode private information."
                           + "",
             tags = "vault",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secrets listed.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.responses.RespSecretList.class))),
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
     public Response listSecretMeta(@PathParam("secretName") String secretName,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "listSecretMeta", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         SkSecretList info = null;
         try {
             info = getVaultImpl().secretListMeta(threadContext.getTenantId(), threadContext.getUser(),
                                                  secretName);
         } catch (Exception e) {                  
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         var r = new RespSecretList(info);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_READ", "Secret", info.secretPath), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* destroySecretMeta:                                                           */
     /* ---------------------------------------------------------------------------- */
     @DELETE
     @Path("/secret/destroy/meta/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Erase all traces of a secret: its key, all versions of its "
                           + "value and all its metadata. "
                           + "Specifying a folder erases all secrets in that folder.",
             tags = "vault",
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret completely removed.",
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
     public Response destroySecretMeta(@PathParam("secretName") String secretName,
                                       @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "readSecretMeta", _request.getRequestURL());
             _log.trace(msg);
         }
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the tenant and user are both assigned.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         Response resp = checkTenantUser(threadContext, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         try {
             getVaultImpl().secretDestroyMeta(threadContext.getTenantId(), 
                                              threadContext.getUser(), 
                                              secretName);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         var r = new RespBasic();
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "Secret", secretName), prettyPrint, r)).build();
     }
}

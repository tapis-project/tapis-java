package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.requestBody.ReqValidateServicePwd;
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
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultAuthorized;
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
    private static final String FILE_SK_VALIDATE_SERVICE_PWD_REQUEST = 
        "/edu/utexas/tacc/tapis/security/api/jsonschema/ValidateServicePwdRequest.json";
    
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
     
     // Map of url secret type text to secret type enum.
     private static final HashMap<String,SecretType> _secretTypeMap = initSecretTypeMap();
    
     /* **************************************************************************** */
     /*                                Public Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* readSecret:                                                                  */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/secret/{secretType}/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Read a versioned secret. "
                           + "By default, the latest version of the secret is read. If the "
                           + "*version* query parameter is specified then that version of the "
                           + "secret is read.  The *version* parameter should be passed as an "
                           + "integer with zero indicating the latest version of the secret. "
                           + "A NOT FOUND status code is returned if the secret version does "
                           + "not exist or if it's deleted or destroyed.\n\n"
                           + ""
                           + "The response object includes the map of zero or more key/value "
                           + "pairs and metadata that describes the secret. The metadata includes "
                           + "which version of the secret was returned.\n\n"
                           + ""
                           + "A valid tenant and user must be specified as query parameters.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* and *secretName* path parameters and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
                           + "",
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
     public Response readSecret(@PathParam("secretType") String secretType,
                                @PathParam("secretName") String secretName,
                                @QueryParam("tenant") String tenant,
                                @QueryParam("user") String user,
                                @DefaultValue("0") @QueryParam("version") int version,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                /* Query parameters used to construct the secret path in vault */
                                @QueryParam("sysid")      String sysId,
                                @QueryParam("sysuser")    String sysUser,
                                @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                @QueryParam("dbhost")     String dbHost,
                                @QueryParam("dbname")     String dbName,
                                @QueryParam("dbservice")  String dbService)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "readSecret", _request.getRequestURL());
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
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         SkSecret skSecret = null;
         try {
             skSecret = getVaultImpl().secretRead(tenant, user, secretPathParms, version);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // ------------------------ Request Output ----------------------------
         // Create the response object.
         var respSecret = new RespSecret(skSecret);
         
         // Success.
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_READ", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, respSecret)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* writeSecret:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/{secretType}/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Create or update a secret. "
                           + "The JSON payload contains a required *data* object and an optional "
                           + "*options* object.  It also contains the required tenant and user "
                           + "fields.\n\n"
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
                           + "parameter.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* and *secretName* path parameters and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
                           + "",
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
     public Response writeSecret(@PathParam("secretType") String secretType,
                                 @PathParam("secretName") String secretName,
                                 @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                 /* Query parameters used to construct the secret path in vault */
                                 @QueryParam("sysid")      String sysId,
                                 @QueryParam("sysuser")    String sysUser,
                                 @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                 @QueryParam("dbhost")     String dbHost,
                                 @QueryParam("dbname")     String dbName,
                                 @QueryParam("dbservice")  String dbService,
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
         String tenant = payload.tenant;
         String user   = payload.user;
         var secretMap = new HashMap<String,Object>();
         if (payload.data != null) secretMap.putAll(payload.data);
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         SkSecretMetadata skSecretMeta = null;
         try {
             skSecretMeta = getVaultImpl().secretWrite(tenant, user, secretPathParms, secretMap);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespSecretMeta r = new RespSecretMeta(skSecretMeta);
         return Response.status(Status.CREATED).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_CREATED", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* deleteSecret:                                                                */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/delete/{secretType}/{secretName}")
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
                           + "   * [-] - empty = delete all versions\n"
                           + "   * [0] - zero = delete only the latest version\n"
                           + "   * [1, 3, ...] - list = delete the specified versions\n\n"
                           + ""
                           + "A valid tenant and user must also be specified in the body.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* and *secretName* path parameters and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
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
     public Response deleteSecret(@PathParam("secretType") String secretType,
                                  @PathParam("secretName") String secretName,
                                  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                  /* Query parameters used to construct the secret path in vault */
                                  @QueryParam("sysid")      String sysId,
                                  @QueryParam("sysuser")    String sysUser,
                                  @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                  @QueryParam("dbhost")     String dbHost,
                                  @QueryParam("dbname")     String dbName,
                                  @QueryParam("dbservice")  String dbService,
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
         String tenant = payload.tenant;
         String user   = payload.user;
         List<Integer> versions = 
             payload.versions != null ? payload.versions : new ArrayList<>(); 
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         List<Integer> deletedVersions = null;
         try {
             deletedVersions = getVaultImpl().secretDelete(tenant, user, secretPathParms, 
                                                           versions);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespVersions r = new RespVersions(deletedVersions);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* undeleteSecret:                                                              */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/undelete/{secretType}/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Restore one or more versions of a secret that have previously been deleted. "
                           + "This endpoint undoes soft deletions performed using the "
                           + "*secret/delete/{secretType}/{secretName}* endpoint.\n\n"
                           + ""
                           + "The input versions array is interpreted as follows:\n\n"
                           + ""
                           + "   * [-] - empty = undelete all versions\n"
                           + "   * [0] - zero = undelete only the latest version\n"
                           + "   * [1, 3, ...] - list = undelete the specified versions\n\n"
                           + ""
                           + "A valid tenant and user must be specified in the body.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* and *secretName* path parameters and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
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
     public Response undeleteSecret(@PathParam("secretType") String secretType,
                                    @PathParam("secretName") String secretName,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    /* Query parameters used to construct the secret path in vault */
                                    @QueryParam("sysid")      String sysId,
                                    @QueryParam("sysuser")    String sysUser,
                                    @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                    @QueryParam("dbhost")     String dbHost,
                                    @QueryParam("dbname")     String dbName,
                                    @QueryParam("dbservice")  String dbService,
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
         String tenant = payload.tenant;
         String user   = payload.user;
         List<Integer> versions = 
             payload.versions != null ? payload.versions : new ArrayList<>(); 
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         List<Integer> undeletedVersions = null;
         try {
             undeletedVersions = getVaultImpl().secretUndelete(tenant, user, secretPathParms, 
                                                               versions);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespVersions r = new RespVersions(undeletedVersions);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_UNDELETED", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* destroySecret:                                                               */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/destroy/{secretType}/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Destroy one or more versions of a secret. Destroy implements "
                           + "a hard delete which delete that cannot be undone. It does "
                           + "not, however, remove any metadata associated with the secret.\n\n"
                           + ""
                           + "The input versions array is interpreted as follows:\n\n"
                           + ""
                           + "   * [-] - empty = destroy all versions\n"
                           + "   * [0] - zero = destroy only the latest version\n"
                           + "   * [1, 3, ...] - list = destroy the specified versions\n\n"
                           + ""
                           + "A valid tenant and user must be specified in the body.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* and *secretName* path parameters and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
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
     public Response destroySecret(@PathParam("secretType") String secretType,
                                   @PathParam("secretName") String secretName,
                                   @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                   /* Query parameters used to construct the secret path in vault */
                                   @QueryParam("sysid")      String sysId,
                                   @QueryParam("sysuser")    String sysUser,
                                   @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                   @QueryParam("dbhost")     String dbHost,
                                   @QueryParam("dbname")     String dbName,
                                   @QueryParam("dbservice")  String dbService,
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
         String tenant = payload.tenant;
         String user   = payload.user;
         List<Integer> versions = 
             payload.versions != null ? payload.versions : new ArrayList<>(); 
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         List<Integer> destroyedVersions = null;
         try {
             destroyedVersions = getVaultImpl().secretDestroy(tenant, user, secretPathParms, 
                                                              versions);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         RespVersions r = new RespVersions(destroyedVersions);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* readSecretMetadata:                                                          */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/secret/read/meta/{secretType}/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "List a secret's metadata including its version information. "
                         + "The input parameter must be a secret name, not a folder. "
                         + "The result includes which version of the secret is the latest.\n\n"
                         + ""
                         + "A valid tenant and user must be specified as query parameters.\n\n"
                         + ""
                         + "### Naming Secrets\n"
                         + ""
                         + "Secrets can be arranged hierarchically by using the \"+\" "
                         + "characters in the *secretName*.  These characters will be "
                         + "converted to slashes upon receipt, allowing secrets to be "
                         + "arranged in folders.\n\n"
                         + ""
                         + "A secret is assigned a path name constructed from the "
                         + "*secretType* and *secretName* path parameters and, optionally, "
                         + "from query parameters determined by the *secretType*. Each "
                         + "*secretType* determines a specific transformation from the url "
                         + "path to a path in the vault.  The *secretType* may require "
                         + "certain query parameters to be present on the request "
                         + "in order to construct the vault path.  See the next "
                         + "section for details.\n\n"
                         + ""
                         + "### Secret Types\n"
                         + ""
                         + "The list below documents each *secretType* and their applicable "
                         + "query parameters. Highlighted parameter names indicate required "
                         + "parameters. When present, default values are listed first and also "
                         + "highlighted.\n\n"
                         + "  - **system**\n"
                         + "    - *sysid*: the unique system id\n"
                         + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                         + "    - keytype: *sshkey* | password | accesskey | cert\n"
                         + "  - **dbcred**\n"
                         + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                         + "    - *dbname*:  the database name or alias\n"
                         + "    - *dbservice*: service name\n"
                         + "  - **jwtsigning** - *no query parameters*\n"
                         + "  - **user** - *no query parameters*\n"
                         + "  - **service** - *no query parameters*\n"
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
     public Response readSecretMeta(@PathParam("secretType") String secretType,
                                    @PathParam("secretName") String secretName,
                                    @QueryParam("tenant") String tenant,
                                    @QueryParam("user")   String user,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    /* Query parameters used to construct the secret path in vault */
                                    @QueryParam("sysid")      String sysId,
                                    @QueryParam("sysuser")    String sysUser,
                                    @DefaultValue("sshkey") @QueryParam("keytype")  String keyType,
                                    @QueryParam("dbhost")     String dbHost,
                                    @QueryParam("dbname")     String dbName,
                                    @QueryParam("dbservice")  String dbService)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "readSecretMeta", _request.getRequestURL());
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
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         SkSecretVersionMetadata info = null;
         try {
             info = getVaultImpl().secretReadMeta(tenant, user, secretPathParms);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         var r = new RespSecretVersionMetadata(info);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_READ", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* listSecretMeta:                                                              */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/secret/list/meta/{secretType}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "List the secret names at the specified path. "
                           + "The path must represent a folder, not an actual secret name. "
                           + "If the path does not have a trailing slash one will be inserted. "
                           + "Secret names should not encode private information.\n\n"
                           + ""
                           + "A valid tenant and user must be specified as query parameters.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the secret name.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* path parameter and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
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
     public Response listSecretMeta(@PathParam("secretType") String secretType,
                                    @QueryParam("tenant") String tenant,
                                    @QueryParam("user")   String user,
                                    @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                    /* Query parameters used to construct the secret path in vault */
                                    @QueryParam("sysid")      String sysId,
                                    @QueryParam("sysuser")    String sysUser,
                                    @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                    @QueryParam("dbhost")     String dbHost,
                                    @QueryParam("dbname")     String dbName,
                                    @QueryParam("dbservice")  String dbService)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "listSecretMeta", _request.getRequestURL());
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
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, null, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         SkSecretList info = null;
         try {
             info = getVaultImpl().secretListMeta(tenant, user, secretPathParms);
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
     @Path("/secret/destroy/meta/{secretType}/{secretName}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Erase all traces of a secret: its key, all versions of its "
                           + "value and all its metadata. "
                           + "Specifying a folder erases all secrets in that folder.\n\n"
                           + ""
                           + "A valid tenant and user must be specified as query parameters.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n"
                           + ""
                           + "A secret is assigned a path name constructed from the "
                           + "*secretType* and *secretName* path parameters and, optionally, "
                           + "from query parameters determined by the *secretType*. Each "
                           + "*secretType* determines a specific transformation from the url "
                           + "path to a path in the vault.  The *secretType* may require "
                           + "certain query parameters to be present on the request "
                           + "in order to construct the vault path.  See the next "
                           + "section for details.\n\n"
                           + ""
                           + "### Secret Types\n"
                           + ""
                           + "The list below documents each *secretType* and their applicable "
                           + "query parameters. Highlighted parameter names indicate required "
                           + "parameters. When present, default values are listed first and also "
                           + "highlighted.\n\n"
                           + "  - **system**\n"
                           + "    - *sysid*: the unique system id\n"
                           + "    - *sysuser*: the accessing user (except when keytype=cert)\n"
                           + "    - keytype: *sshkey* | password | accesskey | cert\n"
                           + "  - **dbcred**\n"
                           + "    - *dbhost*:  the DBMS hostname, IP address or alias\n"
                           + "    - *dbname*:  the database name or alias\n"
                           + "    - *dbservice*: service name\n"
                           + "  - **jwtsigning** - *no query parameters*\n"
                           + "  - **user** - *no query parameters*\n"
                           + "  - **service** - *no query parameters*\n"
                           + "",
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
     public Response destroySecretMeta(@PathParam("secretType") String secretType,
                                       @PathParam("secretName") String secretName,
                                       @QueryParam("tenant") String tenant,
                                       @QueryParam("user")   String user,
                                       @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                                       /* Query parameters used to construct the secret path in vault */
                                       @QueryParam("sysid")      String sysId,
                                       @QueryParam("sysuser")    String sysUser,
                                       @DefaultValue("sshkey") @QueryParam("keytype") String keyType,
                                       @QueryParam("dbhost")     String dbHost,
                                       @QueryParam("dbname")     String dbName,
                                       @QueryParam("dbservice")  String dbService)
     {
         // Trace this request.
         if (_log.isTraceEnabled()) {
             String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), 
                                          "readSecretMeta", _request.getRequestURL());
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
         
         // ------------------------- Path Processing --------------------------
         // Null response means the secret type and its required parameters are present.
         SecretPathMapperParms secretPathParms;
         try {secretPathParms = getSecretPathParms(secretType, secretName, sysId, sysUser,
                                                   keyType, dbHost, dbName, dbService);}
             catch (Exception e) {
                 _log.error(e.getMessage(), e);
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // ------------------------ Request Processing ------------------------
         // Issue the vault call.
         try {
             getVaultImpl().secretDestroyMeta(tenant, user, secretPathParms);
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return getExceptionResponse(e, e.getMessage(), prettyPrint);
         }
         
         // Return the data portion of the vault response.
         var r = new RespBasic();
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "Secret", secretPathParms.getSecretName()), 
                                 prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* validateServicePassword:                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/secret/validateServicePassword/{secretName}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Validate a service's password. "
                           + "The JSON payload contains the password that needs to be validated "
                           + "against the password stored in the vault for the service specified"
                           + "in the X-Tapis-User header. The secret name is the path under which"
                           + "the password was stored.\n\n"
                           + ""
                           + "A valid tenant and user must also be specified in the payload.\n\n"
                           + ""
                           + "### Naming Secrets\n"
                           + ""
                           + "Secrets can be arranged hierarchically by using the \"+\" "
                           + "characters in the *secretName*.  These characters will be "
                           + "converted to slashes upon receipt, allowing secrets to be "
                           + "arranged in folders.\n\n",
             tags = "vault",
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.security.api.requestBody.ReqValidateServicePwd.class))),
             responses = 
                 {@ApiResponse(responseCode = "200", description = "Secret written.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespAuthorized.class))),
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
     public Response validateServicePassword(@PathParam("secretName") String secretName,
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
         ReqValidateServicePwd payload = null;
         try {payload = getPayload(payloadStream, FILE_SK_VALIDATE_SERVICE_PWD_REQUEST, 
                                   ReqValidateServicePwd.class);
         } 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "validateServicePassword", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
               entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Extract input values.
         String tenant = payload.tenant;
         String user   = payload.user;
         
         // Support secret name paths by replacing the escape characters (+) with
         // slashes.  This is typically handled in SecretPathMapperParms. 
         if (secretName != null) secretName = secretName.replace('+', '/');
         
         // ------------------------- Check Tenant -----------------------------
         // Null means the jwt tenant and user are validated.
         Response resp = checkTenantUser(tenant, user, prettyPrint);
         if (resp != null) return resp;
         
         // ------------------------ Request Processing ------------------------
         // Get the names.
         boolean authorized;
         try {authorized = getVaultImpl().validateServicePwd(tenant, user, secretName, 
                                                             payload.password);}
             catch (Exception e) {
                 // Already logged.
                 return getExceptionResponse(e, e.getMessage(), prettyPrint);
             }
         
         // Password was not matched.
         if (!authorized) {
             String msg = MsgUtils.getMsg("SK_INVALID_SERVICE_PASSWORD", 
                                          tenant, user, secretName);
             _log.warn(msg);
             return Response.status(Status.FORBIDDEN).
                 entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Set the result payload on success.
         ResultAuthorized authResp = new ResultAuthorized();
         authResp.isAuthorized = true;
         RespAuthorized r = new RespAuthorized(authResp);
         
         // Return the data portion of the vault response.
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_AUTHORIZED", "Service", secretName), prettyPrint, r)).build();
     }
     
     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* getSecretPathParms:                                                          */
     /* ---------------------------------------------------------------------------- */
     /** Wrap all possible input optional parameters into a single object.  Convert
      * any plus signs (+) in the secretName parameter to slashes (/) as defined on 
      * the client interface.
      * 
      * @return a single parameter object
      * @throws TapisImplException on an invalid secret type
      */
     private SecretPathMapperParms getSecretPathParms(String secretType, String secretName, 
                                         String sysId, String sysUser, String keyType, 
                                         String dbHost, String dbName, String dbService) 
      throws TapisImplException
     {
         // Assign the secret type.
         var secretTypeEnum = _secretTypeMap.get(secretType.toLowerCase());
         if (secretTypeEnum == null) {
             var typeArray = new ArrayList<String>(_secretTypeMap.keySet());
             Collections.sort(typeArray);
             
             // Throw the exception.
             String msg = MsgUtils.getMsg("TAPIS_SECURITY_INVALID_SECRET_TYPE",
                                          secretType, typeArray.toString());
             _log.error(msg);
             throw new TapisImplException(msg, Condition.BAD_REQUEST);
         }

         // Create the parm container object.
         SecretPathMapperParms parms = new SecretPathMapperParms(secretTypeEnum);
         
         // Translate the "+" sign into slashes to allow for vault subdirectories.
         if (secretName != null) secretName = secretName.replace('+', '/');
         
         // Assign the rest of the parm fields.
         parms.setSecretName(secretName);
         parms.setSysId(sysId);
         parms.setSysUser(sysUser);
         parms.setKeyType(keyType);
         parms.setDbHost(dbHost);
         parms.setDbName(dbName);
         parms.setDbService(dbService);
         
         return parms;
     }

     /* ---------------------------------------------------------------------------- */
     /* initSecretTypeMap:                                                           */
     /* ---------------------------------------------------------------------------- */
     /** Initialize a map with key secret type url text and value SecretType enumeration. 
      * 
      * @return the map of text to enum
      */
     private static HashMap<String,SecretType> initSecretTypeMap()
     {
         // Get a map of secret type text to secret type enum. The secret
         // type text is what should appear in url paths.
         SecretType[] types = SecretType.values();
         var map = new HashMap<String,SecretType>(1 + types.length * 2);
         for (int i = 0; i < types.length; i++) map.put(types[i].getUrlText(), types[i]);
         return map;
     }
}

package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bettercloud.vault.VaultException;

import edu.utexas.tacc.tapis.security.api.requestBody.IReqBody;
import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.security.authz.impl.VaultImpl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext.AccountType;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

class AbstractResource 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(AbstractResource.class);

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Role name validator.  Require names to start with alphabetic characters and 
    // be followed by zero or more alphanumeric characters and underscores.  Note that
    // in particular special characters are disallowed by this regex.
    private static final Pattern _namePattern = Pattern.compile("^\\p{Alpha}(\\p{Alnum}|_)*");
    
    /* **************************************************************************** */
    /*                             Protected Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getPayload:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Get the request's json payload and use it to populate a request-specified 
     * object.
     * 
     * @param <T> the result type
     * @param payloadStream the request payload stream returned by jaxrs
     * @param schemaFile the schema file that validates the request json
     * @param classOfT the class of the result object
     * @return the object that received the payload
     * @throws TapisException on error
     */
    protected <T extends IReqBody> 
        T getPayload(InputStream payloadStream, String schemaFile, Class<T> classOfT)
     throws TapisException
    {
        // There better be a payload.
        String json = null;
        try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_IO_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
          }
        
        // Create validator specification.
        JsonValidatorSpec spec = new JsonValidatorSpec(json, schemaFile);
        
        // Make sure the json conforms to the expected schema.
        try {JsonValidator.validate(spec);}
          catch (TapisJSONException e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
          }

        // Populate the result object with the json values.
        T payload = null;
        try {payload = TapisGsonUtils.getGson().fromJson(json, classOfT);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());            
                _log.error(msg, e);
                throw new TapisException(msg, e);
            }
        
        // Validate the payload parameters.
        String msg = payload.validate();
        if (msg != null) {
            _log.error(msg);
            throw new TapisException(msg);
        }
        
       return payload; 
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getRoleImpl:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected RoleImpl getRoleImpl() {return RoleImpl.getInstance();}
    
    /* ---------------------------------------------------------------------------- */
    /* getUserImpl:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected UserImpl getUserImpl() {return UserImpl.getInstance();}
    
    /* ---------------------------------------------------------------------------- */
    /* getRoleImpl:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected VaultImpl getVaultImpl() {return VaultImpl.getInstance();}
    
    /* ---------------------------------------------------------------------------- */
    /* checkTenant:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Check that the threadlocal cache has valid JWT information.  Use that 
     * information to check that the tenant and user specified as request parameters
     * are authorized for the requester.
     * 
     * Return null if the check succeed, otherwise return the error response.
     * 
     * @param tenant the tenant explicitly passed as a request parameter
     * @param user the user explicitly passed as a request parameter, can be null
     * @param prettyPrint whether to pretty print the response or not
     * @return null on success, a response on error
     */
    protected Response checkTenantUser(String tenant, String user, boolean prettyPrint)
    {
        // Get the thread local context and validate context parameters.  The
        // tenantId and user are set in the jaxrc filter classes that process
        // each request before processing methods are invoked.
        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        if (!threadContext.validate()) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
          _log.error(msg);
          return Response.status(Status.BAD_REQUEST).
              entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
        
        // Unpack some jwt information for convenience.
        AccountType accountType = threadContext.getAccountType();
        String jwtTenant = threadContext.getJwtTenantId();
        String jwtUser   = threadContext.getJwtUser();
        
        // Validation depends on the account type.
        if (accountType == AccountType.user) {
            // Compare the tenant values for user accounts.
            if (tenant != null) 
                if (!jwtTenant.equals(tenant)) {
                    String msg = MsgUtils.getMsg("SK_UNEXPECTED_TENANT_VALUE", 
                                                 jwtTenant, tenant, accountType.name());
                    _log.error(msg);
                    return Response.status(Status.BAD_REQUEST).
                        entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
            }
        
            // Compare the user values.
            if (user != null) 
                if (!jwtUser.equals(user)) {
                    String msg = MsgUtils.getMsg("SK_UNEXPECTED_USER_VALUE", 
                                                 jwtUser, user, accountType.name());
                    _log.error(msg);
                    return Response.status(Status.BAD_REQUEST).
                        entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
                }
        }
        else if (tenant != null) {
            // Service accounts are allowed more latitude than user accounts.
            // Specifically, they can specify tenants other than that in their jwt. 
            boolean allowedTenant;
            try {allowedTenant = TapisRestUtils.isAllowedTenant(jwtTenant, tenant);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_SECURITY_ALLOWABLE_TENANT_ERROR", 
                                                 jwtUser, jwtTenant, tenant);
                    _log.error(msg, e);
                    return Response.status(Status.INTERNAL_SERVER_ERROR).
                            entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
                }

            // Can the new tenant id be used by the jwt tenant?
            if (!allowedTenant) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_TENANT_NOT_ALLOWED", 
                                             jwtUser, jwtTenant, tenant);
                _log.error(msg);
                return Response.status(Status.BAD_REQUEST).
                        entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
            }
        }
        
        // Success
        return null;
    }

    /* ---------------------------------------------------------------------------- */
    /* getExceptionResponse:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** Construct responses when exceptions are thrown from a securitylib call.  If 
     * the message parameter is null the message in the exception is logged.
     * 
     * @param e the thrown exception
     * @param message a message to log or null
     * @param prettyPrint whether the response should be pretty printed
     * @param parms 0 or more exception-specific string parameters
     * @return a response
     */
    protected Response getExceptionResponse(Exception e, String message, 
                                            boolean prettyPrint, String... parms)
    {
        // Select and print a message and the caller's stack frame info.
        String msg = message == null ? e.getMessage() : message;
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        String caller = stack.length > 1 ? ("\n   " + stack[2]) : "";
        _log.error(msg + caller);
        
        // Send response based on the type of exception.
        if (e instanceof TapisNotFoundException) {
            
            // Create the response payload.
            TapisNotFoundException e2 = (TapisNotFoundException) e;
            ResultName missingName = new ResultName();
            missingName.name = e2.missingName;
            RespName r = new RespName(missingName);
            
            // Get the not found message parameters.
            String missingType  = parms.length > 0 ? parms[0] : "entity";
            String missingValue = parms.length > 1 ? parms[1] : missingName.name;
            
            return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
                MsgUtils.getMsg("TAPIS_NOT_FOUND", missingType, missingValue), 
                                prettyPrint, r)).build();
        }
        else if (e instanceof TapisImplException) {
            return Response.status(SKApiUtils.toHttpStatus(((TapisImplException)e).condition)).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        } 
        else if (e instanceof VaultException) {
            return Response.status(((VaultException)e).getHttpStatusCode()).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        } 
        else {
            return Response.status(Status.INTERNAL_SERVER_ERROR).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
    }
}

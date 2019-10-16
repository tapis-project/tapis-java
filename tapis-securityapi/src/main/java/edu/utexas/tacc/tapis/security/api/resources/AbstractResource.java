package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.api.responseBody.BodyName;
import edu.utexas.tacc.tapis.security.api.responses.RespName;
import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
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
    // be followed by zero or more alphanumeric characters and underscores.
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
    protected <T> T getPayload(InputStream payloadStream, String schemaFile, Class<T> classOfT)
     throws TapisException
    {
        // There better be a payload.
        String json = null;
        try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "security", e.getMessage());
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
    /* checkTenantUser:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Check that we have value tenant id and user names in the threadlocal cache.
     * Return null if the check succeed, otherwise return the error response.
     * 
     * @param prettyPrint whether to pretty print the response or not
     * @return null on success, a response on error
     */
    protected Response checkTenantUser(TapisThreadContext threadContext, boolean prettyPrint)
    {
        // Get the thread local context and validate context parameters.  The
        // tenantId and user are set in the jaxrc filter classes that process
        // each request before processing methods are invoked.
        if (!threadContext.validate()) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
          _log.error(msg);
          return Response.status(Status.BAD_REQUEST).
              entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
        
        // Success
        return null;
    }

    /* ---------------------------------------------------------------------------- */
    /* isValidName:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Check a candidate name against the name regex.
     * 
     * @param name the name to validate
     * @return true if matches regex, false otherwise
     */
    protected boolean isValidName(String name)
    {
        if (name == null) return false;
        return _namePattern.matcher(name).matches();
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
            BodyName missingName = new BodyName();
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
        else {
            return Response.status(Status.INTERNAL_SERVER_ERROR).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
    }
}

package edu.utexas.tacc.tapis.jobs.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.requestBody.IReqBody;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
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
    /*                             Protected Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getJobsImpl:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected JobsImpl getJobsImpl() {return JobsImpl.getInstance();}
    
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
    /* checkSameTenant:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Check that the threadlocal cache has valid JWT information.  Use that 
     * information to check that the tenant parameter is the same as the tenant
     * specified in the jwt.
     * 
     * Return null if the check succeed, otherwise return the error response.
     * 
     * @param tenant the tenant explicitly passed as a request parameter
     * @param prettyPrint whether to pretty print the response or not
     * @return null on success, a response on error
     */
    protected Response checkSameTenant(String tenant, boolean prettyPrint)
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
        
        // Compare the tenant in the JWT to the tenant parameter.
        String jwtTenant = threadContext.getJwtTenantId();
        if (!jwtTenant.equals(tenant)) {
            String jwtUser   = threadContext.getJwtUser();
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_TENANT_NOT_ALLOWED", 
                                         jwtUser, jwtTenant, tenant);
            _log.error(msg);
            return Response.status(Status.BAD_REQUEST).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
            
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
            return Response.status(JobsApiUtils.toHttpStatus(((TapisImplException)e).condition)).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        } 
        else {
            return Response.status(Status.INTERNAL_SERVER_ERROR).
                entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
    }
}

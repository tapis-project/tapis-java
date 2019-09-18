package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

class AbstractResource 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(AbstractResource.class);

    /* **************************************************************************** */
    /*                                Public Methods                                */
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
    /* allNullOrNot:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** Return true if either both parameters are null or both are not null.
     * Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @return true if both parameters have the same nullity, false otherwise
     */
    protected boolean allNullOrNot(Object o1, Object o2)
    {
        if (o1 == null && o2 == null) return true;
        if (o1 != null && o2 != null) return true;
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* allNullOrNot:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** Return true if either all parameters are null or all are not null.
     * Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @param o3 yet another object
     * @return true if all parameters have the same nullity, false otherwise
     */
    protected boolean allNullOrNot(Object o1, Object o2, Object o3)
    {
        if (o1 == null && o2 == null && o3 == null) return true;
        if (o1 != null && o2 != null && o3 != null) return true;
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* allNull:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Return true if both parameters are null. Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @return true if both parameters are null, false otherwise
     */
    protected boolean allNull(Object o1, Object o2)
    {
        if (o1 == null && o2 == null) return true;
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* allNull:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Return true if all parameters are null. Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @param o3 yet another object
     * @return true if all parameters are null, false otherwise
     */
    protected boolean allNull(Object o1, Object o2, Object o3)
    {
        if (o1 == null && o2 == null && o3 == null) return true;
        return false;
    }
}

package edu.utexas.tacc.tapis.sharedapi.utils;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper.RESPONSE_STATUS;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;

/** This class is a replacement for the RestUtils class in the same package.
 * This class provides an alternative way to generate JSON responses to HTTP
 * calls that is amenable to openapi code generation.  When RestUtils is no
 * longer referenced we should delete it.
 * 
 * @author rcardone
 */
public class TapisRestUtils 
{
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* createSuccessResponse:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Return the json string that represent the response to a REST call that succeeds
     * and returns a result object.
     * 
     * @param message the application level error message to be included in the response
     * @param prettyPrint true for multi-line formating, false for compact formatting
     * @param result a result object to be converted to json
     * @return the json response string
     */
    public static String createSuccessResponse(String message, boolean prettyPrint, RespAbstract resp)
    {
        // Fill in the base fields.
        resp.status = RESPONSE_STATUS.success.name();
        resp.message = message;
        resp.version = TapisUtils.getTapisVersion();
        return TapisGsonUtils.getGson(prettyPrint).toJson(resp);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createSuccessResponse:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Return the json string that represent the response to a REST call that succeeds.
     * 
     * @param message the application level error message to be included in the response
     * @param prettyPrint true for multi-line formating, false for compact formatting
     * @return the json response string
     */
    public static String createSuccessResponse(String message, boolean prettyPrint)
    {
        // Fill in the base fields.
        RespBasic resp = new RespBasic();
        resp.status = RESPONSE_STATUS.success.name();
        resp.message = message;
        resp.version = TapisUtils.getTapisVersion();
        return TapisGsonUtils.getGson(prettyPrint).toJson(resp);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createErrorResponse:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Return the json string that represent the response to a REST call that 
     * experienced an error and returns a result object.
     * 
     * @param message the application level error message to be included in the response
     * @param prettyPrint true for multi-line formating, false for compact formatting
     * @param result a result object to be converted to json
     * @return the json response string
     */
    public static String createErrorResponse(String message, boolean prettyPrint, RespAbstract resp)
    {
        // Fill in the base fields.
        resp.status = RESPONSE_STATUS.error.name();
        resp.message = message;
        resp.version = TapisUtils.getTapisVersion();
        return TapisGsonUtils.getGson(prettyPrint).toJson(resp);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createErrorResponse:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Return the json string that represent the response to a REST call that 
     * experienced an error.
     * 
     * @param message the application level error message to be included in the response
     * @param prettyPrint true for multi-line formating, false for compact formatting
     * @return the json response string
     */
    public static String createErrorResponse(String message, boolean prettyPrint)
    {
        // Fill in the base fields.
        RespBasic resp = new RespBasic();
        resp.status = RESPONSE_STATUS.error.name();
        resp.message = message;
        resp.version = TapisUtils.getTapisVersion();
        return TapisGsonUtils.getGson(prettyPrint).toJson(resp);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* checkJWTSubjectFormat:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Return true if the subject is non-null and of the form user@tenant, false 
     * otherwise. 
     * 
     * @param subject a subject in the form user@tenant
     * @return true is valid, false otherwise
     */
    public static boolean checkJWTSubjectFormat(String subject)
    {
        // Null or empty string don't cut it.
        if (StringUtils.isBlank(subject)) return false;
        
        // Test for non-whitespace characters on both sides of the @ sign.
        String trimmedSubject = subject.trim();
        int index = trimmedSubject.indexOf("@");
        if (index < 1 || (index >= trimmedSubject.length() - 1)) return false;
        
        // Correct format.
        return true;
    }
}

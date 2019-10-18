package edu.utexas.tacc.tapis.sharedapi.utils;

import java.util.HashMap;

import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper.RESPONSE_STATUS;

public class RestUtils 
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(RestUtils.class);
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // Mapping of exception class names to HTTP status response codes.  The names are
  // those returned from getClass().getName().  See the initialization method for a
  // note concerning maintenance.
  private static final HashMap<String,Status> _exceptionStatuses = initExceptionStatuses();
  
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
   * 
   * @deprecated Replaced by method in TapisRestUtils
   */
  @Deprecated(forRemoval = true)
  public static String createSuccessResponse(String message, boolean prettyPrint, Object result)
  {
    ResponseWrapper wrapper = 
        new ResponseWrapper(RESPONSE_STATUS.success, message);
    return wrapper.addResult(result, prettyPrint);
  }
  
  /* ---------------------------------------------------------------------------- */
  /* createSuccessResponse:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Return the json string that represent the response to a REST call that succeeds.
   * 
   * @param message the application level error message to be included in the response
   * @param prettyPrint true for multi-line formating, false for compact formatting
   * @return the json response string
   * 
   * @deprecated Replaced by method in TapisRestUtils
   */
  @Deprecated(forRemoval = true)
  public static String createSuccessResponse(String message, boolean prettyPrint)
  {
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.success, message);
    return TapisGsonUtils.getGson(prettyPrint).toJson(wrapper);
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
   * 
   * @deprecated Replaced by method in TapisRestUtils
   */
  @Deprecated(forRemoval = true)
  public static String createErrorResponse(String message, boolean prettyPrint, Object result)
  {
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.error, message);
    return wrapper.addResult(result, prettyPrint);  
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
   * 
   * @deprecated Replaced by method in TapisRestUtils
   */
  @Deprecated(forRemoval = true)
  public static String createErrorResponse(String message, boolean prettyPrint)
  {
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.error, message);
    return TapisGsonUtils.getGson(prettyPrint).toJson(wrapper);
  }

  /* ---------------------------------------------------------------------------- */
  /* getStatus:                                                                   */
  /* ---------------------------------------------------------------------------- */
  /** Return the HTTP status code in a response based on the exception generated by
   * a request.  If an exception type is not recognized INTERNAL_SERVER_ERROR
   * is returned.
   * 
   * Note that keeping this code outside of the exception classes themselves keep
   * HTTP knowledge outside the non-api code. 
   *  
   * @param t the exception caused during the processing of an HTTP request
   * @return the status associated with the exception type or INTERNAL_SERVER_ERROR
   */
  public static Status getStatus(Throwable t)
  {
    return getStatus(t, Status.INTERNAL_SERVER_ERROR);
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getStatus:                                                                   */
  /* ---------------------------------------------------------------------------- */
  /** Return the HTTP status code in a response based on the exception generated by
   * a request.  If an exception type is not recognized the default status parameter
   * is returned.
   *  
   * Note that keeping this code outside of the exception classes themselves keep
   * HTTP knowledge outside the non-api code. 
   *  
   * @param t the exception caused during the processing of an HTTP request
   * @param defaultStatus the status returned by default for unrecognized exceptions
   * @return the status associated with the exception type or the default status
   */
  public static Status getStatus(Throwable t, Status defaultStatus)
  {
    // This should never happen.
    if ((t == null) || (defaultStatus == null)) return Status.INTERNAL_SERVER_ERROR;
    
    // See if this exception has been mapped.
    Status status = _exceptionStatuses.get(t.getClass().getName());
    if (status != null) return status;
      else return defaultStatus;
  }
  
  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* initExceptionStatuses:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Create the mapping used by getStatus() to select a HTTP status codes given an 
   * exception.  The names are those returned from getClass().getName().
   * 
   * MAINTENANCE NOTE:  If an exception's name or package changes and it has an
   *                    entry in the map below, then that entry must also change.
   *                    
   * DESIGN NOTE:  Other designs considered for mapping exceptions to HTTP codes
   *  are listed below.  Maybe there's a better way to do this, but what we have
   *  now is clear and fast despite the extra maintenance on rare occasions.
   *  
   *  A) Carry the HTTP code in the exception.
   *      - Requires mix HTTP/REST notions in library code that is otherwise request
   *        oblivious.  Doesn't handle non-Tapis exception well.
   *  B) Use exception type as map key.
   *      - Requires putting all Tapis exceptions in the shared library.
   *  C) Compare strings using an IF or SWITCH statement in getStatus() rather than a map.
   *      - Same maintenance issue as current map approach except slower.
   *  D) Compare class objects (i.e., addresses) using IF or SWITCH in getStatus().
   *      - Same issues as A).  
   * 
   * @return the exception to status mapping
   */
  private static HashMap<String,Status> initExceptionStatuses()
  {
    // Set the capacity to be about twice the number of entries to avoid rehashing.
    HashMap<String,Status> map = new HashMap<>(23);
    map.put("edu.utexas.tacc.tapis.shared.exceptions.TapisException", Status.INTERNAL_SERVER_ERROR);
    map.put("edu.utexas.tacc.tapis.shared.exceptions.TapisRuntimeException", Status.INTERNAL_SERVER_ERROR);
    
    map.put("edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException", Status.INTERNAL_SERVER_ERROR);
    map.put("edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException", Status.BAD_REQUEST);
    map.put("edu.utexas.tacc.tapis.shared.exceptions.TapisUUIDException", Status.INTERNAL_SERVER_ERROR);

    map.put("edu.utexas.tacc.tapis.jobs.exceptions.JobException", Status.INTERNAL_SERVER_ERROR);
    map.put("edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException", Status.INTERNAL_SERVER_ERROR);
    map.put("edu.utexas.tacc.tapis.jobs.exceptions.JobQueueFilterException", Status.BAD_REQUEST);
    map.put("edu.utexas.tacc.tapis.jobs.exceptions.JobQueuePriorityException", Status.BAD_REQUEST);
    map.put("edu.utexas.tacc.tapis.jobs.exceptions.JobInputException", Status.BAD_REQUEST);
    
    return map;
  }
}

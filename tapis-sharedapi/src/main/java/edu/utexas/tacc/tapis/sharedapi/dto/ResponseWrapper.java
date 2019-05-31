package edu.utexas.tacc.tapis.sharedapi.dto;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** This class provides a wrapper for all REST responses.
 * The fields defined here are the first fields in a the
 * JSON response which may or may not contain additional
 * JSON objects depending on the command.
 * 
 * @author rcardone
 */
public class ResponseWrapper 
{
  /* **************************************************************************** */
  /*                                    Enums                                     */
  /* **************************************************************************** */
  // Possible status field values.
  public enum RESPONSE_STATUS {success, error}
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  private RESPONSE_STATUS status;
  private String          message;
  private String          version;
  
  /* **************************************************************************** */
  /*                                 Constructors                                 */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
  public ResponseWrapper()
  {
    // Get the version number captured at build time from the pom file.
    this.version = TapisUtils.getTapisVersion();
  }
  
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
  public ResponseWrapper(RESPONSE_STATUS status, String message)
  {
    this();
    this.status  = status;
    this.message = message;
  }
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* addResult:                                                                   */
  /* ---------------------------------------------------------------------------- */
  /** Returns a json string consisting of this object's contents followed by the 
   * result object's content. 
   * 
   * @param resultObject the Java object whose contents need to be added to this 
   *            response wrapper
   * @param prettyPrint true for multi-line pretty printing, false otherwise
   * @return a json response string
   */
  public String addResult(Object resultObject, boolean prettyPrint)
  {
    JsonObject obj = addResult(resultObject);
    return TapisGsonUtils.getGson(prettyPrint).toJson(obj);
  }
  
  /* ---------------------------------------------------------------------------- */
  /* addResult:                                                                   */
  /* ---------------------------------------------------------------------------- */
  /** Returns a gson object consisting of this object's contents followed by the 
   * result object's content.
   *  
   * @param resultObject the Java object that contains result information
   * @return the combined wrapper and result gson object
   */
  public JsonObject addResult(Object resultObject)
  {
    // Get the gson generator.
    Gson gson = TapisGsonUtils.getGson();
    JsonObject obj = (JsonObject) gson.toJsonTree(this);
    
    // Add in the result object's json if it exists.
    if (resultObject != null) 
        obj.add("result", gson.toJsonTree(resultObject));
    return obj;
  }
  
  /* **************************************************************************** */
  /*                                  Accessors                                   */
  /* **************************************************************************** */
  public String getVersion() {
    return version;
  }
  public void setVersion(String version) {
    this.version = version;
  }
  public RESPONSE_STATUS getStatus() {
    return status;
  }
  public void setStatus(RESPONSE_STATUS status) {
    this.status = status;
  }
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }
  
}

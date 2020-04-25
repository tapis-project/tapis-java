package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.ws.rs.core.EntityTag;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CoreResponse {
  
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(CoreResponse.class);

  // private boolean isResponseValid;
  private Map<String, List<String>> headers;
  private String method;
  private String coreResponsebody;
  private String coreMsg;
  private int statusCode;
  private String etag;
  private String location;
  private boolean basicResponse;
  
  /**
   * map the response from the core server request to our jaxrs response framework.
   * @param coreResponse
   */
  public void mapResponse(okhttp3.Response coreResponse) {
    // http method
    captureResponseMethod(coreResponse);
    
    // collect all the response headers
    captureCoreResponseHeaders(coreResponse);
    
    // collect the message returned from core
    captureResponseMsg(coreResponse);
    
    // collect the response body for return back to requestor
    captureResponseBody(coreResponse);
    
    // what was our status code returned
    statusCode = coreResponse.code();
    
  }
  
  /*************************************************
   *    Capture methods for core server Response
   *************************************************/
  private void captureCoreResponseHeaders(okhttp3.Response coreResponse) {
    _log.debug("Capture Headers from core response ...");
    headers = coreResponse.headers().toMultimap();
    this.etag = coreResponse.header("ETag");
    this.location = coreResponse.header("Location");
    
    _log.debug(logResponseHeaders());
  }
  
  private void captureResponseBody(okhttp3.Response coreResponse) {
    _log.debug("response body output ");
    ResponseBody responseBody = coreResponse.body();
    try {
      coreResponsebody = responseBody.string();
    } catch (IOException e) {
      _log.debug("response body exception thrown");
      e.printStackTrace();
    }
  }
  
  private void captureResponseMethod(okhttp3.Response coreResponse) {
    method = coreResponse.request().method();
  }
  
  private void captureResponseMsg(okhttp3.Response coreResponse){
    coreMsg = coreResponse.message();
  }
  
  private void captureStatusCode(okhttp3.Response coreResponse){
    statusCode = coreResponse.code();
  }
  
  public String getEtagValueFromHeaders(){
    String etagValue = null;
    if(headers.containsKey("ETag")){
      List<String> etagList = headers.get("ETag");
      etagValue = etagList.get(0);
    }
    return etagValue;
  }
  
  public String getLocationFromHeaders(){
    String locationValue = null;
    if(headers.containsKey("Location")){
      List<String> locationList = headers.get("Location");
      locationValue = locationList.get(0);
    }
    return locationValue;
  }
  
  /*************************************************
*     Print functions for core server Response
 ************************************************/
 
  private String logResponseHeaders() {
    StringBuilder sb = new StringBuilder();
    sb.append("Headers from core ...");
    Iterator<Map.Entry<String, List<String>>> iterator = headers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, List<String>> entry = iterator.next();
      _log.debug(entry.getKey() + ":" + entry.getValue());
    }
    return sb.toString();
  }
  
  private void logResponseBody() {
    _log.debug("response body output ");
    _log.debug("size of response body : " + coreResponsebody);
    if (coreResponsebody.length() > 0) {
      _log.debug("response : \n" + coreResponsebody.toString());
    }
  }
  
  protected String getBasicResponse(){
    RespBasic resp = new RespBasic();
    resp.status = String.valueOf(this.getStatusCode());
    resp.message = this.coreMsg;
    resp.version = TapisUtils.getTapisVersion();
    resp.result = this.coreResponsebody;
    return TapisGsonUtils.getGson().toJson(resp);
  }
  
  private void printMethod() {
    _log.debug("http method used : " + this.method);
  }
  
  private void printResponseMsg() {
    _log.debug("http msg returned : " + coreMsg);
  }
  
  /*************************************************
   *   Getters and Setters
   *************************************************/

  public Map<String, List<String>> getHeaders() { return headers; }
  
  public String getCoreResponsebody() {
    return coreResponsebody;
  }
  
  public void setCoreResponsebody(String coreResponsebody) {
    this.coreResponsebody = coreResponsebody;
  }
  
  public String getMethod() {
    return method;
  }
  
  public void setMethod(String method) {
    this.method = method;
  }
  
  public int getStatusCode() {
    return statusCode;
  }
  
  public void setStatusCode(int statusCode) {
    this.statusCode = statusCode;
  }
  
  public String getEtag() { return etag; }
  
  public void setEtag(String etag) { this.etag = etag; }
  
  public boolean isBasicResponse() { return basicResponse; }
  
  public void setBasicResponse(boolean basicResponse) { this.basicResponse = basicResponse; }
  
  public String getLocation() { return location; }
  
  public void setLocation(String location) { this.location = location; }
  
}

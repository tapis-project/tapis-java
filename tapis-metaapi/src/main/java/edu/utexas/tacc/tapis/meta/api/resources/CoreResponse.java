package edu.utexas.tacc.tapis.meta.api.resources;

import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.Map;

public class CoreResponse {
  
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(CoreResponse.class);

  // private boolean isResponseValid;
  private MultivaluedMap headers;
  private String method;
  private StringBuilder coreResponsebody;
  private StringBuilder coreMsg;
  private int statusCode;
  
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
    Map headers = coreResponse.headers().toMultimap();
    headers.forEach((k, v) -> _log.debug((k + ":" + v)));
    
  }
  
  private void captureResponseBody(okhttp3.Response coreResponse) {
    _log.debug("response body output ");
    ResponseBody responseBody = coreResponse.body();
    try {
      coreResponsebody = new StringBuilder(responseBody.string());
    } catch (IOException e) {
      _log.debug("response body exception thrown");
      e.printStackTrace();
    }
  }
  
  private void captureResponseMethod(okhttp3.Response coreResponse) {
    method = coreResponse.request().method();
  }
  
  private void captureResponseMsg(okhttp3.Response coreResponse){
    coreMsg = new StringBuilder(coreResponse.message());
  }
  
  private void captureStatusCode(okhttp3.Response coreResponse){
    statusCode = coreResponse.code();
  }
  
  
/*************************************************
*     Print functions for core server Response
 *************************************************/
  private void printResponseHeaders() {
    _log.debug("Headers from core ...");
    headers.forEach((k, v) -> _log.debug((k + ":" + v)));
  }
  
  private void printResponseBody() {
    _log.debug("response body output ");
    _log.debug("size of response body : " + coreResponsebody);
    if (coreResponsebody.length() > 0) {
      _log.debug("response : \n" + coreResponsebody.toString());
    }
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
  public StringBuilder getCoreResponsebody() {
    return coreResponsebody;
  }
  
  public void setCoreResponsebody(StringBuilder coreResponsebody) {
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
}

package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.meta.config.OkSingleton;
import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.meta.utils.MetaAppConstants;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;


/**
 * this class is responsible for proxying the requests from the security container sidecar to the core container
 *
 */
public class CoreRequest {
  private static final long serialVersionUID = 2267124925495540982L;
  
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(CoreRequest.class);
  
  // fields
  private static String coreServiceUrl;  // look at a static init block
  private OkHttpClient okHttpClient = OkSingleton.getInstance();
  private String pathUri;
  private String pathURL;
  
  // constructor(s)
  public CoreRequest(String _pathUri){
    // this gives us the valid Core server path URI by stripping the service
    // prefix from the beginning of the path
    pathUri = _pathUri.replace(MetaAppConstants.META_REQUEST_PREFIX,"");
    pathURL = RuntimeParameters.getInstance().getCoreServer()+pathUri;
  }
  
  // proxy GET request
  public CoreResponse proxyGetRequest(){
    // path url here has stripped out /v3/meta to make the correct path request
    //  to core server
    okhttp3.Request coreRequest = new Request.Builder()
        .url(pathURL)
        .build();
  
    Response response = null;
    CoreResponse coreResponse = new CoreResponse();
    try {
      response = okHttpClient.newCall(coreRequest).execute();
      coreResponse.mapResponse(response);
      StringBuilder sb = coreResponse.getCoreResponsebody();
      
    } catch (IOException e) {
      // todo log message
      // todo throw a custom exception about request failure to core
      e.printStackTrace();
    }
    
    _log.debug("call to host : "+pathURL+"\n"+"response : \n"+coreResponse.getCoreResponsebody().toString());
    
    return coreResponse;
  }
  
  // proxy PUT request
  public okhttp3.Response proxyPutRequest(){
    return null;
  }
  
  // proxy POST request
  public okhttp3.Response proxyPostRequest(){
    return null;
  }
  
  // proxy DELETE request
  public okhttp3.Response proxyDeleteRequest(){
    return null;
  }
  
  
}

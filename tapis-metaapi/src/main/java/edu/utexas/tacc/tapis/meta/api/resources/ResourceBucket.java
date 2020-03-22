package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.meta.config.OkSingleton;
import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;


@Path("/")
public class ResourceBucket {
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(ResourceBucket.class);
  
  @Context
  private HttpHeaders _httpHeaders;
  
  @Context
  private Application _application;
  
  @Context
  private UriInfo _uriInfo;
  
  @Context
  private SecurityContext _securityContext;
  
  @Context
  private ServletContext _servletContext;
  
  @Context
  private HttpServletRequest _request;
  
  @GET
  @Path("/")
  public javax.ws.rs.core.Response listDBs() {
    // todo implement
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "listDBs", _request.getRequestURL());
      _log.trace(msg);
    }
    String result = "TODO";
    return javax.ws.rs.core.Response.status(javax.ws.rs.core.Response.Status.OK).entity(result).build();
    
  }
  
  @GET
  @Path("/{db}")
  public javax.ws.rs.core.Response listCollections(@PathParam("db") String db) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "listCollections", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("List collections in " + db);
    }
    
    // Proxy the GET request and handle any exceptions
    // we will always return a response for a request that means something
    CoreResponse result = proxyGETRequest();
    // javax.ws.rs.core.Response jaxResponse = buildResponse(result);
    
    
    // ---------------------------- Response -------------------------------
    return javax.ws.rs.core.Response.status(result.getStatusCode()).entity(result.getCoreResponsebody()).build();
  }
  
  @GET
  @Path("/{db}/{collection}")
  public javax.ws.rs.core.Response listDocuments(@PathParam("db") String db,
                                                 @PathParam("collection") String collection) {
    // todo implement
    
    // ---------------------------- Success -------------------------------
    // Success means we found the document list.
    String result = "";
    return javax.ws.rs.core.Response.status(javax.ws.rs.core.Response.Status.OK).entity(result).build();
    
  }
  
  // -----------------------------------------------------------
  //  Proxy call handling
  // -----------------------------------------------------------
  
  /**
   * @return
   */
  private CoreResponse proxyGETRequest() {
    _log.debug("Handling a GET request. ");
    
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    return coreResponse;
  }
  
  private javax.ws.rs.core.Response buildResponse(CoreResponse result) {
    // first let's just pass the result along
    return null;
  }
  
  private void requestDump() {
    
    String pathUri = _request.getRequestURI();
    StringBuffer pathUrl = _request.getRequestURL();
    String queryString = _request.getQueryString();
    String contextPath = _request.getContextPath();
    
    
  }
  
}


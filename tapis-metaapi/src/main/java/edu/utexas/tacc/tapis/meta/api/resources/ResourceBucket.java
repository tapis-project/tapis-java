package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.meta.config.OkSingleton;
import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


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
  
  /*************************************************
   *    Root endpoints
   *************************************************/
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
  
  /*************************************************
   *    Database (DB) endpoints
   *************************************************/
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
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
 
  /*************************************************
   *    Colllection endpoints
   *************************************************/
  @GET
  @Path("/{db}/{collection}")
  public javax.ws.rs.core.Response listDocuments(@PathParam("db") String db,
                                                 @PathParam("collection") String collection) {
    // Proxy the GET request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
  
  @POST
  @Path("/{db}/{collection}")
  @Consumes(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createDocument(InputStream payload) {
    // Get the json payload to proxy to back end
    StringBuilder builder = new StringBuilder();
  
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line = null;
      while ((line = in.readLine()) != null) {
        builder.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
    }
  
    _log.debug("Data Received: " + builder.toString());

    // Proxy the POST request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPostRequest(builder.toString());
    
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
  
  
  
  /*************************************************
   *    Document endpoints
   *************************************************/
  @GET
  @Path("/{db}/{collection}/{documentId}")
  public javax.ws.rs.core.Response getDocument(@PathParam("db") String db,
                                                 @PathParam("collection") String collection, @PathParam("documentId") String documentId) {
    // Proxy the GET request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
  
  
  
  
  
  private void requestDump() {
    
    String pathUri = _request.getRequestURI();
    StringBuffer pathUrl = _request.getRequestURL();
    String queryString = _request.getQueryString();
    String contextPath = _request.getContextPath();
    
  }
  
}


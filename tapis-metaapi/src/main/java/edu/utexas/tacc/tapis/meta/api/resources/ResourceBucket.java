package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.meta.config.OkSingleton;
import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.glassfish.jersey.server.internal.routing.UriRoutingContext;
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
import java.net.URI;


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
  //----------------  List DBs in server ----------------
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
  
  //----------------  List Collections in DB ----------------
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
   *    Collection endpoints
   *************************************************/
  
  //----------------  Create a Collection ----------------
  @PUT
  @Path("/{db}/{collection}")
  @Consumes(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createCollection(@PathParam("db") String db, @PathParam("collection") String collection) {
  
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "createCollection", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("create collection "+collection+" in " + db);
    }
    
    // Proxy the PUTT request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest("{}");
  
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
  
  //----------------  List documents in Collection ----------------
  @GET
  @Path("/{db}/{collection}")
  public javax.ws.rs.core.Response listDocuments(@PathParam("db") String db,
                                                 @PathParam("collection") String collection,
                                                 @QueryParam("filter") String filter) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "listDocuments", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("List documents in " + db +"/"+collection);
    }
  
    StringBuffer pathUrl = new StringBuffer(_request.getRequestURI());
    pathUrl.append("?"+_request.getQueryString());
    
    // Proxy the GET request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
  
  //----------------  Create a document ----------------
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
   *    Index endpoints
   *************************************************/

  //----------------  List Indexes ----------------
  @GET
  @Path("/{db}/{collection}/_indexes")
  public javax.ws.rs.core.Response listIndexes(@PathParam("db") String db,
                                               @PathParam("collection") String collection) {
  
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "listIndexes", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("List indexes in " + db +"/"+collection);
    }
  
    StringBuffer pathUrl = new StringBuffer(_request.getRequestURI());
    pathUrl.append("?"+_request.getQueryString());
  
    // Proxy the GET request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
  }
  
  //----------------  Create an Index ----------------
  @PUT
  @Path("/{db}/{collection}/_indexes/{indexName}")
  @Consumes(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createIndex(@PathParam("db") String db,
                                               @PathParam("collection") String collection,
                                               @PathParam("indexName") String indexName,
                                               InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "createIndex", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Create index "+indexName+" in " + db +"/"+collection);
    }
  
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
    CoreResponse coreResponse = coreRequest.proxyPutRequest(builder.toString());
    
    // ---------------------------- Response -------------------------------
    // right now we are just returning whatever the backend core server sends back
    // this may need a more specific translation for more informative messages
    // especially in case of an error or an exception.
    // todo revisit
  
    // Response.ResponseBuilder responseBuilder = javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString());
    // put all the core response headers in our response
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody().toString()).build();
    // return null;
  }
  

  /*************************************************
   *    Document endpoints
   *************************************************/

  //----------------  Get a specific Document ----------------
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
    
    System.out.println(pathUri);
    System.out.println(pathUrl);
    System.out.println(queryString);
    System.out.println(contextPath);
    System.out.println();
    
    // String s = ((UriRoutingContext) _uriInfo).requestContext.requestUri.toString();
  }
  
}


package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedReader;
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
  // TODO ----------------  List DBs in server ----------------
  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response listDBNames() {

    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "listDBs", _request.getRequestURL());
      _log.trace(msg);
    }
  
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  
  /*************************************************
   *    Database (DB) endpoints
   *************************************************/
  
  //----------------  List Collections in DB ----------------
  @GET
  @Path("/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response listCollectionNames(@PathParam("db") String db) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "listCollections", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("List collections in " + db);
    }
    
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Get DB metadata ----------------
  @GET
  @Path("/{db}/_meta")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response getDBMetadata(@PathParam("db") String db) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "getDBMetadata", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Get the Metadata for " + db);
    }
    
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  
  // TODO ----------------  Create DB ----------------
  @PUT
  @Path("/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createDB(){
    return javax.ws.rs.core.Response.status(200).entity("{ TODO }").build();
  }
  
  // TODO ----------------  Delete DB ----------------
  @DELETE
  @Path("/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response deleteDB(){
    return javax.ws.rs.core.Response.status(200).entity("{ TODO }").build();
  }
  
  /*************************************************
   *    Collection endpoints
   *************************************************/
  
  //----------------  Create a Collection ----------------
  @PUT
  @Path("/{db}/{collection}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createCollection(@PathParam("db") String db, @PathParam("collection") String collection) {
  
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "createCollection", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("create collection "+collection+" in " + db);
    }
    
    // Proxy the PUTT request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest("{}");
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  //----------------  List documents in Collection ----------------
  @GET
  @Path("/{db}/{collection}")
  @Produces(MediaType.APPLICATION_JSON)
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
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Get the number of documents in Collection ----------------
  @GET
  @Path("/{db}/{collection}/_size")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response getCollectionSize(@PathParam("db") String db,
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
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Get the collection metadata ----------------
  @GET
  @Path("/{db}/{collection}/_meta")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response getCollectionMetadata(@PathParam("db") String db,
                                                         @PathParam("collection") String collection) {
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
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  //----------------  Create a document ----------------
  @POST
  @Path("/{db}/{collection}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createDocument(@PathParam("db") String db,
                                                  @PathParam("collection") String collection,
                                                  InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "deleteCollection", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Delete collection "+collection+" in " + db );
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
    CoreResponse coreResponse = coreRequest.proxyPostRequest(builder.toString());
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  //----------------  Delete a collection  ----------------
  @DELETE
  @Path("/{db}/{collection}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response deleteCollection(@PathParam("db") String db,
                                                    @PathParam("collection") String collection) {
    
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "deleteCollection", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Delete collection "+collection+" in " + db );
    }
    // Get the json payload to proxy to back end
    
    StringBuilder builder = new StringBuilder();
    
    // Proxy the POST request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyDeleteRequest(_httpHeaders);
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  
  /*************************************************
   *    Index endpoints
   *************************************************/

  //----------------  List Indexes ----------------
  @GET
  @Path("/{db}/{collection}/_indexes")
  @Produces(MediaType.APPLICATION_JSON)
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
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  //----------------  Create an Index ----------------
  @PUT
  @Path("/{db}/{collection}/_indexes/{indexName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
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
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest(builder.toString());
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Delete an Index ----------------
  @DELETE
  @Path("/{db}/{collection}/_indexes/{indexName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response deleteIndex(@PathParam("db") String db,
                                               @PathParam("collection") String collection,
                                               @PathParam("indexName") String indexName) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "deleteIndex", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Delete index "+indexName+" in " + db +"/"+collection);
    }
    
    // Proxy the DELETE request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyDeleteRequest(_httpHeaders);
    
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }

  /*************************************************
   *    Document endpoints
   *************************************************/

  //----------------  Get a specific Document ----------------
  @GET
  @Path("/{db}/{collection}/{documentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response getDocument(@PathParam("db") String db,
                                               @PathParam("collection") String collection,
                                               @PathParam("documentId") String documentId) {
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Put a document ----------------
  @PUT
  @Path("/{db}/{collection}/{documentId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response replaceDocument(@PathParam("db") String db,
                                                 @PathParam("collection") String collection,
                                                 @PathParam("documentId") String documentId,
                                                 InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "putDocument", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Put a document in " + db +"/"+ collection);
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
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest(builder.toString());
    
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Patch a document ----------------
  @PATCH
  @Path("/{db}/{collection}/{documentId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response modifyDocument(@PathParam("db") String db,
                                                 @PathParam("collection") String collection,
                                                 @PathParam("documentId") String documentId,
                                                 InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "patchDocument", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Patch document in " + db +"/"+ collection);
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
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPatchRequest(builder.toString());
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Delete a specific Document ----------------
  @DELETE
  @Path("/{db}/{collection}/{documentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response deleteDocument(@PathParam("db") String db,
                                               @PathParam("collection") String collection,
                                               @PathParam("documentId") String documentId) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "deleteDocument", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Delete document "+ documentId +" in "+ db +"/"+ collection);
    }
  
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyDeleteRequest(_httpHeaders);
  
    // TODO ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    Response.ResponseBuilder responseBuilder = javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody());
    Response response = responseBuilder.build();
    return response;
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


package edu.utexas.tacc.tapis.meta.api.resources;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
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
import javax.ws.rs.core.*;
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
   *    Information unauthenticated endpoints
   *************************************************/
  
  //----------------  health check ----------------
  @GET
  @Path("/healthcheck")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public javax.ws.rs.core.Response healthCheck() {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "healthcheck", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("check for liveness or a healthy service.");
    }
    
    // Proxy the GET request and handle any exceptions
    // in this case we are just checking the data flow from
    // the core server all the way to mongodb.
    CoreRequest coreRequest = new CoreRequest("/v3/meta/");
    CoreResponse coreResponse = coreRequest.proxyGetRequest();

    String rslt = coreResponse.getBasicResponse();
    

    // return the result from core server
    // If our response status is a 200, all is well
    // If not either the core or mongodb is down.
    // In either case the service is not healthy.
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(rslt).build();
  }
  
  
  /*-------------------------------------------------------
   *                  Root (/) endpoints
   * ------------------------------------------------------*/
  // ----------------  List DBs in server ----------------
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
  
    // ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  
  /*-------------------------------------------------------
   *                   Database (DB) endpoints
   * ------------------------------------------------------*/
 
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
  
    // ---------------------------- Response -------------------------------
    // just return whatever core server sends to us or a basic error response
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
    
    // ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  
  // ----------------  Create DB ----------------
  @PUT
  @Path("/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createDB(@PathParam("db") String db){
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "createDB", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("create database "+ db);
    }
  
    // Proxy the PUT request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest("{}");
  
    //---------------------------- Response -------------------------------
    // return core server response
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
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
  public javax.ws.rs.core.Response createCollection(@PathParam("db") String db,
                                                    @PathParam("collection") String collection) {
  
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "createCollection", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("create collection "+collection+" in " + db);
    }
    
    // Proxy the PUT request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest("{}");
  
    // ---------------------------- Response -------------------------------
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

    if(!StringUtils.isEmpty(_request.getQueryString())){
      pathUrl.append("?"+_request.getQueryString());
    }
    
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(pathUrl.toString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    //---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  //----------------  large query submission ----------------
  // this endpoint takes a valid mongodb query document and submits it to
  // the core server
  @POST
  @Path("/{db}/{collection}/_filter")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response submitLargeQuery(@PathParam("db") String db,
                                                    @PathParam("collection") String collection,
                                                    @QueryParam("page") String page,
                                                    @QueryParam("pagesize") String pagesize,
                                                    InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "submitLargeQuery", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Submit a query larger than the url query param limitation for HTTP to " + collection);
    }
    
    // Get the json payload to proxy to back end
    StringBuilder jsonPayloadToProxy = new StringBuilder();
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
    
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());
    
    // we change here from a POST request to a GET request.
    // RH core will except URL GET filter request without the URL limitation.
    String inComingRequest = _request.getRequestURI();
    inComingRequest = inComingRequest.replace("_filter", "?filter=");
    StringBuilder newUriPath = new StringBuilder(); ///meta/v3/v1airr/rearrangement/_filter
    newUriPath.append(inComingRequest)
              .append(jsonPayloadToProxy.toString()).append("&" + _request.getQueryString() + "&sort={}");
     

    CoreRequest coreRequest = new CoreRequest(newUriPath.toString());
    CoreResponse coreResponse = coreRequest.proxyPostRequest(jsonPayloadToProxy.toString());
    
    String result;
    result = coreResponse.getCoreResponsebody();
    
    // ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    Response.ResponseBuilder responseBuilder = javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(
        result);
    
    return responseBuilder.build();
    
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
    
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI() + "?" + _request.getQueryString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    // ---------------------------- Response -------------------------------
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
    
    // StringBuffer pathUrl = new StringBuffer(_request.getRequestURI());
    // pathUrl.append("?"+_request.getQueryString());
    
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI() + "?" + _request.getQueryString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
    
    // ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  //----------------  Create a document ----------------
  // this endpoint also returns the oid of a newly created document as an ETag header
  @POST
  @Path("/{db}/{collection}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response createDocument(@PathParam("db") String db,
                                                  @PathParam("collection") String collection,
                                                  @QueryParam("basic") boolean basic,
                                                  InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "createDocument", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Create document  in " + collection );
    }
  
    // Get the json payload to proxy to back end
    StringBuilder jsonPayloadToProxy = new StringBuilder();
  
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line = null;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
  
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());

    // Proxy the POST request and handle any exceptions
    // we will always return a response for a request that means something
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPostRequest(jsonPayloadToProxy.toString());
  
    String result = null;
    String etag = coreResponse.getEtag();
    String location = coreResponse.getLocationFromHeaders();
    
    // if the basic flag is thrown let's get the location header result
    if(basic){
      result = coreResponse.getBasicResponse(location);
    }else {
      result =  coreResponse.getCoreResponsebody();
    }
  
    //---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    Response.ResponseBuilder responseBuilder = javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(result);
    
    if(!StringUtils.isBlank(etag)){
     responseBuilder.tag(etag);
    }
    
    if(!StringUtils.isBlank(location)){
      responseBuilder.header("location",location);
    }
  
    Response response = responseBuilder.build();
  
    return response;
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
    
    StringBuilder jsonPayloadToProxy = new StringBuilder();
    
    // Proxy the POST request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyDeleteRequest(_httpHeaders);
  
    //---------------------------- Response -------------------------------
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
  
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI() + "?" + _request.getQueryString());
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    // ---------------------------- Response -------------------------------
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
    StringBuilder jsonPayloadToProxy = new StringBuilder();
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line = null;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
    
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());
    
    // Proxy the POST request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest(jsonPayloadToProxy.toString());
  
    //---------------------------- Response -------------------------------
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
    
    // ---------------------------- Response -------------------------------
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
  
    // ---------------------------- Response -------------------------------
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
    StringBuilder jsonPayloadToProxy = new StringBuilder();
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line = null;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
    
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());
    
    // Proxy the POST request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest(jsonPayloadToProxy.toString());
    
    //---------------------------- Response -------------------------------
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
    StringBuilder jsonPayloadToProxy = new StringBuilder();
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line = null;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
    
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());
    
    // Proxy the POST request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPatchRequest(jsonPayloadToProxy.toString());
  
    //---------------------------- Response -------------------------------
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
  
    //---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    Response.ResponseBuilder responseBuilder = javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody());
    Response response = responseBuilder.build();
    return response;
  }
  
  /*-------------------------------------------------------
   *     Aggregation endpoints
   * ------------------------------------------------------*/
  
  // ----------------  Put an aggregation ----------------
  @PUT
  @Path("/{db}/{collection}/_aggr/{aggregation}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response addAggregation(@PathParam("db") String db,
                                                   @PathParam("collection") String collection,
                                                   @PathParam("aggregation") String aggregation,
                                                   InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "addAggregation", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Add an aggregation in " + db +"/"+ collection);
    }
  
    // Get the json payload to proxy to back end
    StringBuilder jsonPayloadToProxy = new StringBuilder();
  
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line = null;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
  
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());
  
    // Proxy the PUT request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyPutRequest(jsonPayloadToProxy.toString());
  
    //---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Use an aggregation ----------------
  @GET
  @Path("/{db}/{collection}/_aggrs/{aggregation}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response useAggregation(@PathParam("db") String db,
                                                  @PathParam("collection") String collection,
                                                  @PathParam("aggregation") String aggregation,
                                                  @QueryParam("avars") String avars) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "getAggregation", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Get aggregation " + aggregation + " in " + db + "/" + collection);
      _log.trace("avars: " + avars);
    }
    
    // Proxy the GET request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI() + "?avars=" + avars);
    CoreResponse coreResponse = coreRequest.proxyGetRequest();
  
    //---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  Post large aggregation avars ----------------
  @POST
  @Path("/{db}/{collection}/_aggrs/{aggregation}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response submitLargeAggregation(@PathParam("db") String db,
                                                          @PathParam("collection") String collection,
                                                          @PathParam("aggregation") String aggregation,
                                                          @QueryParam("page") String page,
                                                          @QueryParam("pagesize") String pagesize,
                                                          InputStream payload) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "addAggregation", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Add an aggregation in " + db + "/" + collection);
    }
    // we want to capture the oversize avars parameter in a request body
    // to use as a submission to RH core server in a GET.
    
    // Get the json payload to proxy to back end
    StringBuilder jsonPayloadToProxy = new StringBuilder();
    // we will assign the payload to an avars Query parameter to RH core server.
    jsonPayloadToProxy.append("?avars=");
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(payload));
      String line;
      while ((line = in.readLine()) != null) {
        jsonPayloadToProxy.append(line);
      }
    } catch (Exception e) {
      _log.debug("Error Parsing: - ");
      // TODO
    }
    
    _log.debug("Data Received: " + jsonPayloadToProxy.toString());
    
    String inComingRequest = _request.getRequestURI();
    StringBuilder newUriPath = new StringBuilder(); ///meta/v3/v1airr/rearrangement/_aggrs/facets
    newUriPath.append(inComingRequest)
              .append(jsonPayloadToProxy.toString()).append("&" + _request.getQueryString());

    // Proxy the POST request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(newUriPath.toString());
    CoreResponse coreResponse = coreRequest.proxyPostRequest(jsonPayloadToProxy.toString());
    
    //---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
  }
  
  // ----------------  delete an aggregation ----------------
  @DELETE
  @Path("/{db}/{collection}/_aggr/{aggregation}")
  @Produces(MediaType.APPLICATION_JSON)
  public javax.ws.rs.core.Response deleteAggregation(@PathParam("db") String db,
                                                     @PathParam("collection") String collection,
                                                     @PathParam("aggregation") String aggregation,
                                                     @QueryParam("avars") String agvars) {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(),
          "deleteAggregation", _request.getRequestURL());
      _log.trace(msg);
      _log.trace("Delete aggregation in " + db + "/" + collection);
    }
  
    // Proxy the DELETE request and handle any exceptions
    CoreRequest coreRequest = new CoreRequest(_request.getRequestURI());
    CoreResponse coreResponse = coreRequest.proxyDeleteRequest(_httpHeaders);
  
    // ---------------------------- Response -------------------------------
    // just return whatever core server sends to us
    return javax.ws.rs.core.Response.status(coreResponse.getStatusCode()).entity(coreResponse.getCoreResponsebody()).build();
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


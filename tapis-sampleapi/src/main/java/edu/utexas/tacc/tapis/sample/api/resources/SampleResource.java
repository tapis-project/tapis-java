package edu.utexas.tacc.tapis.sample.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.sample.dao.SampleDao;
import edu.utexas.tacc.tapis.sample.model.Sample;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@Path("/")
public class SampleResource 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SampleResource.class);
    
    // Json schema resource files.
    private static final String FILE_SAMPLE_CREATE_REQUEST = 
        "/edu/utexas/tacc/tapis/sample/api/jsonschema/SampleCreateRequest.json";
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    /* Jax-RS context dependency injection allows implementations of these abstract
     * types to be injected (ch 9, jax-rs 2.0):
     * 
     *      javax.ws.rs.container.ResourceContext
     *      javax.ws.rs.core.Application
     *      javax.ws.rs.core.HttpHeaders
     *      javax.ws.rs.core.Request
     *      javax.ws.rs.core.SecurityContext
     *      javax.ws.rs.core.UriInfo
     *      javax.ws.rs.core.Configuration
     *      javax.ws.rs.ext.Providers
     * 
     * In a servlet environment, Jersey context dependency injection can also 
     * initialize these concrete types (ch 3.6, jersey spec):
     * 
     *      javax.servlet.HttpServletRequest
     *      javax.servlet.HttpServletResponse
     *      javax.servlet.ServletConfig
     *      javax.servlet.ServletContext
     *
     * Inject takes place after constructor invocation, so fields initialized in this
     * way can not be accessed in constructors.
     */ 
     @Context
     private HttpHeaders        _httpHeaders;
  
     @Context
     private Application        _application;
  
     @Context
     private UriInfo            _uriInfo;
  
     @Context
     private SecurityContext    _securityContext;
  
     @Context
     private ServletContext     _servletContext;
  
     @Context
     private HttpServletRequest _request;
    
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* hello:                                                                       */
  /* ---------------------------------------------------------------------------- */
  @GET
  @Path("/hello")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiResponse(description = "A greeting")
  public Response getDummy(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
  {
      // Trace this request.
      if (_log.isTraceEnabled()) {
          String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hello", 
                                     "  " + _request.getRequestURL());
          _log.trace(msg);
      }
         
      // ---------------------------- Success ------------------------------- 
      // Success means we found the job. 
      return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_FOUND", "hello", "no items"), prettyPrint, "Hello from the Tapis Sample application.")).build();
  }

  /* ---------------------------------------------------------------------------- */
  /* sample:                                                                      */
  /* ---------------------------------------------------------------------------- */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response sample(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                         InputStream payloadStream)
  {
    // Trace this request.
    if (_log.isTraceEnabled()) {
      String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "post sample", 
                                   "  " + _request.getRequestURL());
      _log.trace(msg);
    }
    
    // ------------------------- Validate Payload -------------------------
    // Read the payload into a string.
    String json = null;
    try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
      catch (Exception e) {
        String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "post sample", e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.BAD_REQUEST).
                entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
    
    // Create validator specification.
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SAMPLE_CREATE_REQUEST);
    
    // Make sure the json conforms to the expected schema.
    try {JsonValidator.validate(spec);}
      catch (TapisJSONException e) {
        String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.BAD_REQUEST).
                entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
  
    // Extract the text value.
    String text = null;
    JsonObject obj = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);
    text = obj.get("text").getAsString(); // validated to be a non-empty string 
    
    // Check text.
    if (StringUtils.isBlank(text)) {
        String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "post sample", "Null or empty text.");
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).
                entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    
    // ------------------------- Create Sample Record ---------------------
    try {
        SampleDao dao = new SampleDao();
        dao.createSample(text);
    }
    catch (Exception e) {
        String msg = MsgUtils.getMsg("SAMPLE_INSERT_TEXT_ERROR", e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.BAD_REQUEST).
                entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    
    // ---------------------------- Success ------------------------------- 
    // Success means we found the job. 
    return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
        MsgUtils.getMsg("SAMPLE_CREATED", text), prettyPrint, "Inserted record into database")).build();
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getSampleById:                                                               */
  /* ---------------------------------------------------------------------------- */
  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSampleById(@PathParam("id") int id,
                                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
  {
      // Trace this request.
      if (_log.isTraceEnabled()) {
          String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSample", 
                                       "  " + _request.getRequestURL());
        _log.trace(msg);
      }
      
      // ------------------------- Retrieve Job -----------------------------
      // Retrieve the specified job if it exists.
      SampleDao dao = new SampleDao();
      Sample sample = null;
      try {sample = dao.getSampleById(id);}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("SAMPLE_SELECT_ID_ERROR", id, 
                                           e.getMessage());
              _log.error(msg, e);
              return Response.status(RestUtils.getStatus(e)).
                  entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
          }
      
      // The specified job was not found for the tenant.
      if (sample == null) {
          String msg = MsgUtils.getMsg("SAMPLE_NOT_FOUND", id);
          _log.warn(msg);
          return Response.status(Status.NOT_FOUND).
                  entity(RestUtils.createErrorResponse(MsgUtils.getMsg("TAPIS_NOT_FOUND", "Sample", id), 
                         prettyPrint)).build();
      }
      
      // ---------------------------- Success ------------------------------- 
      // Success means we found the job. 
      return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_FOUND", "Sample", id), prettyPrint, sample)).build();
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getSamples:                                                                  */
  /* ---------------------------------------------------------------------------- */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSamples(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
  {
      // Trace this request.
      if (_log.isTraceEnabled()) {
          String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSample", 
                                       "  " + _request.getRequestURL());
        _log.trace(msg);
      }
      
      // ------------------------- Retrieve Job -----------------------------
      // Retrieve the specified job if it exists.
      SampleDao dao = new SampleDao();
      List<Sample> samples = null;
      try {samples = dao.getSamples();}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("SAMPLE_SELECT_ERROR", e.getMessage());
              _log.error(msg, e);
              return Response.status(RestUtils.getStatus(e)).
                  entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
          }
      
      // ---------------------------- Success ------------------------------- 
      // Success means we found the job.
      int cnt = samples == null ? 0 : samples.size();
      return Response.status(Status.OK).entity(RestUtils.createSuccessResponse(
          MsgUtils.getMsg("TAPIS_FOUND", "Samples",  cnt + " items"), prettyPrint, samples)).build();
  }
}

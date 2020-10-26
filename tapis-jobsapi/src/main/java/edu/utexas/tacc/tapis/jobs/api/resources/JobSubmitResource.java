package edu.utexas.tacc.tapis.jobs.api.resources;

import java.io.InputStream;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

public class JobSubmitResource 
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobSubmitResource.class);
    
    // Json schema resource files.
    private static final String FILE_JOB_SUBMIT_REQUEST = 
        "/edu/utexas/tacc/tapis/jobs/api/jsonschema/SubmitJobRequest.json";
    
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
     /* submitJob:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     public Response submitJob(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                               InputStream payloadStream)
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("ALOE_TRACE_REQUEST", getClass().getSimpleName(), "submitJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // ------------------------- Input Processing -------------------------
       // Parse and validate the json in the request payload, which must exist.
       ReqSubmitJob payload = null;
       try {payload = getPayload(payloadStream, FILE_JOB_SUBMIT_REQUEST, 
    		                     ReqSubmitJob.class);
       } 
       catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                        "sbumitJob", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).
             entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }

       
       return null;
     }
}

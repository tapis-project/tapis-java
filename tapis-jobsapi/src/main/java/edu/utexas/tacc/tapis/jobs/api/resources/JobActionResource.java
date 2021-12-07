package edu.utexas.tacc.tapis.jobs.api.resources;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.responses.RespCancelJob;
import edu.utexas.tacc.tapis.jobs.api.responses.RespHideJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.dto.JobCancelDisplay;
import edu.utexas.tacc.tapis.jobs.model.dto.JobHideDisplay;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement; 

@Path("/")
public class JobActionResource extends AbstractResource {

	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobActionResource.class);
    
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
     /* hideJob:                                                                      */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/hide")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Hide a job by its UUID.\n\n"
                           + "The caller must be the job owner, creator or a tenant administrator."
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job was hide.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespCancelJob.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Job not found.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "409", description = "Job is already in Terminal State (CANCELLED, FINISHED or FAILED). Job state conflicts with the cancel request. No action taken",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
     )
     public Response hideJob(@PathParam("jobUuid") String jobUuid,
                            @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                               
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hideJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
           String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "jobUuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                      entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
           var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
           _log.error(msg);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
    // ------------------------- Retrieve Job Status-----------------------------
       JobStatusDTO jobstatus = null;
       var jobsImpl = JobsImpl.getInstance();
       try {
           
           jobstatus = jobsImpl.getJobStatusByUuid(jobUuid, threadContext.getOboUser(),
                                       threadContext.getOboTenantId());
       }
       catch (TapisImplException e) {
           _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }
       catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }

       // ------------------------- Process Results --------------------------
       // Adjust status based on whether we found the job.
       if (jobstatus == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
       
       }
       // ------------------------- Check the Job's status -----------------------------
       // If job is not in terminal state then hiding job cannot be performed.
       
       if(!jobstatus.getStatus().isTerminal()) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           String msg = MsgUtils.getMsg("JOBS_JOB_NOT_IN_TERMINAL_STATE", jobUuid);
           _log.warn(msg);
    	   return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(
                   msg, prettyPrint, r)).build(); 
       }
       // Don't change visibility if already set to hidden
       if(!jobstatus.getVisible()) {
    	   String msg = MsgUtils.getMsg("JOBS_JOB_VISBILITY", jobUuid, "hidden");
       	  
       	   return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            msg, prettyPrint)).build();
       }
       
       //------------------------- Change the visibility  -----------------------------
       // set visible to false
      
		if (!jobsImpl.doHideJob(jobUuid, threadContext.getOboTenantId(), threadContext.getOboUser() ))
		       return Response.status(Status.INTERNAL_SERVER_ERROR).
		               entity(TapisRestUtils.createErrorResponse(MsgUtils.getMsg("JOBS_JOB_UNCHANGED_VISIBILITY", jobUuid, "unhidden"),
		                   prettyPrint)).build();
     
       
       // ---------------------------- Success -------------------------------
       // Success.
       JobHideDisplay hideMsg = new JobHideDisplay();
       String msg = MsgUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "hidden");
       hideMsg.setMessage(msg);
       
       RespHideJob r = new RespHideJob(hideMsg); 
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "hidden"), prettyPrint,r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* unhideJob:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/unhide")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Hide a job by its UUID.\n\n"
                           + "The caller must be the job owner, creator or a tenant administrator."
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job was hide.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespCancelJob.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "404", description = "Job not found.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "409", description = "Job is already in Terminal State (CANCELLED, FINISHED or FAILED). Job state conflicts with the cancel request. No action taken",
                  content = @Content(schema = @Schema(
                     implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
     )
          
     public Response unhideJob(@PathParam("jobUuid") String jobUuid,
                            @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                               
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "unhideJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
           String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "jobUuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                      entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
           var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
           _log.error(msg);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
    // ------------------------- Retrieve Job Status-----------------------------
       JobStatusDTO jobstatus = null;
       var jobsImpl = JobsImpl.getInstance();
       try {
           
           jobstatus = jobsImpl.getJobStatusByUuid(jobUuid, threadContext.getOboUser(),
                                       threadContext.getOboTenantId());
       }
       catch (TapisImplException e) {
           _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }
       catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }

       // ------------------------- Process Results --------------------------
       // Adjust status based on whether we found the job.
       if (jobstatus == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
       
       }
       // ------------------------- Check the Job's status -----------------------------
       // If job is not in terminal state then hiding job cannot be performed.
       
       if(!jobstatus.getStatus().isTerminal()) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           String msg = MsgUtils.getMsg("JOBS_JOB_NOT_IN_TERMINAL_STATE", jobUuid);
           _log.warn(msg);
    	   return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(
                   msg, prettyPrint, r)).build(); 
       }
       // Don't change visibility if already set to unhidden
       if(jobstatus.getVisible()) {
    	   String msg = MsgUtils.getMsg("JOBS_JOB_VISBILITY", jobUuid, "unhidden");
       	  
       	   return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            msg, prettyPrint)).build();
       }
       
       //------------------------- Change the visibility  -----------------------------
       // set visible to true
      
		if (!jobsImpl.doUnHideJob(jobUuid, threadContext.getOboTenantId(), threadContext.getOboUser() ))
		       return Response.status(Status.INTERNAL_SERVER_ERROR).
		               entity(TapisRestUtils.createErrorResponse(MsgUtils.getMsg("JOBS_JOB_UNCHANGED_VISIBILITY", jobUuid, "unhidden"),
		                   prettyPrint)).build();
     
       
       // ---------------------------- Success -------------------------------
       // Success.
       JobHideDisplay hideMsg = new JobHideDisplay();
       String msg = MsgUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "unhidden");
       hideMsg.setMessage(msg);
       
       RespHideJob r = new RespHideJob(hideMsg); 
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "unhidden"), prettyPrint,r)).build();
     }

}

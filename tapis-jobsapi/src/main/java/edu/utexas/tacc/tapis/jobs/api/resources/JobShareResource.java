package edu.utexas.tacc.tapis.jobs.api.resources;


import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqShareJob;
import edu.utexas.tacc.tapis.jobs.api.responses.RespGetJobShareList;
import edu.utexas.tacc.tapis.jobs.api.responses.RespShareJob;
import edu.utexas.tacc.tapis.jobs.api.responses.RespUnShareJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.jobs.model.dto.JobShareDisplay;
import edu.utexas.tacc.tapis.jobs.model.dto.JobShareListDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobUnShareDisplay;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@Path("/")
public class JobShareResource 
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobShareResource.class);
    
    // Json schema resource files.
    private static final String FILE_JOB_SHARE_REQUEST = 
        "/edu/utexas/tacc/tapis/jobs/api/jsonschema/ShareJobRequest.json";
    
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
     /* shareJob:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/share")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Share a job with a user of the tenant. "
            		 + "The caller must be the job owner, creator or a tenant administrator.",
                     
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.requestBody.ReqShareJob.class))),
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job is shared sucessfully.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespShareJob.class))),
                  @ApiResponse(responseCode = "400", description = "Input error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "401", description = "Not authorized.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "403", description = "Forbidden.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
     )
     public Response shareJob(@PathParam("jobUuid") String jobUuid, @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                               InputStream payloadStream)
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "shareJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // The shared code takes it from here.
       String json = null;
       try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
         catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job share", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
       
    
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqShareJob payload = null;
         try {payload = getPayload(json, FILE_JOB_SHARE_REQUEST, ReqShareJob.class);} 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "shareJob", e.getMessage());
             _log.error(msg, e);
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
         
         // ------------------------- Retrieve Job Status Information -----------------------------

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
         
         if (jobstatus == null) {
             ResultName missingName = new ResultName();
             missingName.name = jobUuid;
             RespName r = new RespName(missingName);
             return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
         
         }
                
         // ------------------------- Check Authorizations ----------------------
         boolean authorized = false;
        
         
         //check if the user making the API request is authorized
         // Default authorize user - job owner, admin, anyone who created the job
         authorized = jobsImpl.isAuthorized(jobstatus.getOwner(), threadContext.getOboUser(), threadContext.getOboTenantId(), 
      		   jobstatus.getTenant(), jobstatus.getCreatedBy(), jobstatus.getCreatedByTenant());
         
         if(!authorized) {
      	   String msg = "Either " + MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", threadContext.getOboUser(), jobstatus.getOwner());
      	   msg = " or " + MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", threadContext.getOboTenantId(), jobstatus.getTenant());
      	   
      	   return Response.status(Status.UNAUTHORIZED).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Process Results --------------------------
         if(!jobstatus.getVisible()) {
      	   String msg = MsgUtils.getMsg("JOBS_JOB_NOT_VISIBLE", jobUuid, threadContext.getOboTenantId());
         	   _log.warn(msg);
         	   ResultName missingName = new ResultName();
         	   missingName.name = jobUuid;
         	   RespName r = new RespName(missingName);
         	   return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
              msg, prettyPrint, r)).build();
         }
         
         // ------------------------- Populate JobShared  -----------------------
         // Polulate the in-memory representation of Job shared model
         // Initialize jobShared with calculated effective parameters.
         ArrayList<JobShared> jobsSharedArray = new ArrayList<JobShared>();
         
         List<String> resourceTypeList = payload.getJobResource();
         
         // Parameters for JobShared construtcor:
         // tenant, createdby, jobUuid, grantee, grantor, jobResource, JobTapisPermission  jobPermission
         for(String resourceType: resourceTypeList) { 	 
        	 JobShared jobShared = null;
        	 jobShared = new JobShared(threadContext.getOboTenantId(), threadContext.getOboUser(), 
        			 jobUuid, payload.getGrantee(), jobstatus.getOwner(), JobResourceShare.valueOf(resourceType),
        			 JobTapisPermission.valueOf(payload.getJobPermission()));
             
        	 jobsSharedArray.add(jobShared);
         }
         
         // ------------------------- Save Job Share ---------------------------------
         // Create the job share information and log the share event in the database
                
         for(JobShared jshare : jobsSharedArray ) {
	         try {
	        	 jobsImpl.createShareJob(jshare);
	         } catch (Exception e) {
	             _log.error(e.getMessage(), e);
	             return Response.status(Status.INTERNAL_SERVER_ERROR).
	                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	         }
	         
	        try {
	             // Write to the job_events table
	             jobsImpl.createShareEvent(jshare);
	        } catch (Exception e) {
	             _log.error(e.getMessage(), e);
	             return Response.status(Status.INTERNAL_SERVER_ERROR).
	                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	         }
        
         }
        
         // Success.
         JobShareDisplay shareMsg = new JobShareDisplay();
         String msg = MsgUtils.getMsg("JOBS_JOB_SHARED", jobUuid, threadContext.getOboUser(),
        		 payload.getGrantee(), threadContext.getOboTenantId() );
         shareMsg.setMessage(msg);
       
         RespShareJob r = new RespShareJob(shareMsg);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 msg, prettyPrint, r)).build();
     }
     
     @GET
     @Path("/{jobUuid}/share")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Retrieve share information of a job by its UUID.\n\n"
                           + "The caller must be the job owner, creator or a tenant administrator.",
                           
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job's share information retrieved.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespGetJobShareList.class))),
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
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
     )
     public Response getJobShare(@PathParam("jobUuid") String jobUuid, @QueryParam("limit") int limit, 
				@QueryParam("skip") int skip, @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                               
     { 
    	// Trace this request.
         if (_log.isTraceEnabled()) {
           String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getJobShare", 
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
         
         SearchParameters srchParms = threadContext.getSearchParameters();
         
         if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
         int totalCount = -1; 
         
      
         // ------------------------- Retrieve Job Status Information -----------------------------
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
         
         if (jobstatus == null) {
             ResultName missingName = new ResultName();
             missingName.name = jobUuid;
             RespName r = new RespName(missingName);
             return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
         
         }
                
         // ------------------------- Check Authorizations ----------------------
         boolean authorized = false;
        
         
         //check if the user making the API request is authorized
         // Default authorize user - job owner, admin, anyone who created the job
         authorized = jobsImpl.isAuthorized(jobstatus.getOwner(), threadContext.getOboUser(), threadContext.getOboTenantId(), 
      		   jobstatus.getTenant(), jobstatus.getCreatedBy(), jobstatus.getCreatedByTenant());
         
         if(!authorized) {
      	   String msg = "Either " + MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", threadContext.getOboUser(), jobstatus.getOwner());
      	   msg = " or " + MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", threadContext.getOboTenantId(), jobstatus.getTenant());
      	   
      	   return Response.status(Status.UNAUTHORIZED).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Process Results --------------------------
         if(!jobstatus.getVisible()) {
      	   String msg = MsgUtils.getMsg("JOBS_JOB_NOT_VISIBLE", jobUuid, threadContext.getOboTenantId());
         	   _log.warn(msg);
         	   ResultName missingName = new ResultName();
         	   missingName.name = jobUuid;
         	   RespName r = new RespName(missingName);
         	   return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
              msg, prettyPrint, r)).build();
         }
         
         // --------------------- Get Job's Share Information -------------------
         
         List<JobShareListDTO> shareList = new ArrayList<JobShareListDTO>();
         
         try {
        	 shareList = jobsImpl.getShareJob(jobUuid,jobstatus.getOwner(),threadContext.getOboUser(),threadContext.getOboTenantId());
         } catch(TapisImplException e) {
        	 _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build(); 
         } catch (Exception e) {
             _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }
         
         if(!shareList.isEmpty()) {
        	 totalCount = shareList.size();
         }
         
         RespGetJobShareList r = new RespGetJobShareList(shareList, srchParms.getLimit(), srchParms.getOrderBy(), 
        		 srchParms.getSkip(), srchParms.getStartAfter(), totalCount);
         
        
         if(shareList.isEmpty()) {
        	 String msg =  MsgUtils.getMsg("JOBS_JOB_NO_SHARES_FOUND", jobUuid, threadContext.getOboUser(), threadContext.getOboTenantId());
        	 return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                    msg, prettyPrint,r)).build();
         }
        
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("JOBS_JOB_SHARE_INFO_RETRIEVED", jobUuid, threadContext.getOboUser(), 
                		 threadContext.getOboTenantId()), prettyPrint, r)).build();
    } 
    
     @DELETE
     @Path("/{jobUuid}/share/{user}")
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Delete all share information of a previously shared job for a specific user\n\n"
                           + "The caller must be the job owner, creator or a tenant administrator."
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job's share information deleted for a specific user.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespUnShareJob.class))),
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
                  @ApiResponse(responseCode = "500", description = "Server error.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
     )
     public Response deleteJobShare(@PathParam("jobUuid") String jobUuid, @PathParam("user") String user, 
				@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                               
     { 
    	// Trace this request.
         if (_log.isTraceEnabled()) {
           String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "deleteJobShare", 
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
         
         
         // ------------------------- Retrieve Job Status Information -----------------------------
         JobStatusDTO jobstatus = null;
         var jobsImpl = JobsImpl.getInstance();
         try {
             
             jobstatus = jobsImpl.getJobStatusByUuid(jobUuid, threadContext.getOboUser(), threadContext.getOboTenantId());
          		  
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
         
         if (jobstatus == null) {
             ResultName missingName = new ResultName();
             missingName.name = jobUuid;
             RespName r = new RespName(missingName);
             return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
         
         }
                
         // ------------------------- Check Authorizations ----------------------
         boolean authorized = false;
        
         //check if the user making the API request is authorized
         // Default authorize user - job owner, admin, anyone who created the job
         authorized = jobsImpl.isAuthorized(jobstatus.getOwner(), threadContext.getOboUser(), threadContext.getOboTenantId(), 
      		   jobstatus.getTenant(), jobstatus.getCreatedBy(), jobstatus.getCreatedByTenant());
         
         if(!authorized) {
      	   String msg = "Either " + MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", threadContext.getOboUser(), jobstatus.getOwner());
      	   msg = " or " + MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", threadContext.getOboTenantId(), jobstatus.getTenant());
      	   
      	   return Response.status(Status.UNAUTHORIZED).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Process Results --------------------------
         if(!jobstatus.getVisible()) {
      	   String msg = MsgUtils.getMsg("JOBS_JOB_NOT_VISIBLE", jobUuid, threadContext.getOboTenantId());
         	   _log.warn(msg);
         	   ResultName missingName = new ResultName();
         	   missingName.name = jobUuid;
         	   RespName r = new RespName(missingName);
         	   return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
              msg, prettyPrint, r)).build();
         }
         
         // --------------------- Delete Job's Share Information -------------------
         List<JobShared> getSharedList = null;
         try {
        	 getSharedList = jobsImpl.getSharesJob(user, jobUuid,jobstatus.getOwner(), 
        			 threadContext.getOboUser(), threadContext.getOboTenantId()); // Instead of obouser, it will be job owner
         } catch(Exception e) {
        	 _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }
         
         for(JobShared js: getSharedList) {
        	 try {
	        	 jobsImpl.deleteShareJob(js,threadContext.getOboTenantId(), threadContext.getOboUser());
	         } catch (Exception e) {
	             _log.error(e.getMessage(), e);
	             return Response.status(Status.INTERNAL_SERVER_ERROR).
	                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	         }
	         
	        try {
	             // Write to the event table
	             jobsImpl.createUnShareEvent(js);
	        } catch (Exception e) {
	             _log.error(e.getMessage(), e);
	             return Response.status(Status.INTERNAL_SERVER_ERROR).
	                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	         }
        
         
         }
         
         JobUnShareDisplay unshareMsg = new JobUnShareDisplay();
	     String msg = MsgUtils.getMsg("JOBS_JOB_UNSHARED", jobUuid,threadContext.getOboUser(), user, threadContext.getOboTenantId());
	     unshareMsg.setMessage(msg);
     
	     RespUnShareJob r = new RespUnShareJob(unshareMsg); 
	     return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
	             msg, prettyPrint,r)).build();
    }   
     
}
     



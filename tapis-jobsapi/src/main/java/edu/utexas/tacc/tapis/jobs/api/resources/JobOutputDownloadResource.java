package edu.utexas.tacc.tapis.jobs.api.resources;

import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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

import edu.utexas.tacc.tapis.files.client.FilesClient.StreamedFile;
import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.jobs.utils.DataLocator;
import edu.utexas.tacc.tapis.jobs.utils.JobOutputInfo;
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
public class JobOutputDownloadResource extends AbstractResource{
	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobOutputDownloadResource.class);
    
    private static final int ONE_GB_IN_KB = 1000000;
    private static final int DEFAULT_LIMIT = 100;
    private static final int DEFAULT_SKIP = 0;
    
    
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
     /* getJobOutputDownload:                                                         */
     /* ---------------------------------------------------------------------------- */
    
     @GET
     @Path("/{jobUuid}/output/download/{outputPath: (.*+)}")
     @Produces(MediaType.APPLICATION_OCTET_STREAM)
     @Operation(
             description = "Download job's output files for previously submitted job by its UUID. The job must be in a terminal state (FINISHED or FAILED or CANCELLED).  \n\n"
                           + "The caller must be the job owner, creator or a tenant administrator.\n"
            		       + "The URL must ends with '/' even if there is no outputPath is specified. "
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job's output files downloaded.",content = 
                          @Content(mediaType = "application/octet-stream", schema = @Schema(type = "string", format = "binary"))),
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
     public Response getJobOutputDownload(@PathParam("jobUuid") String jobUuid,@DefaultValue("")@PathParam("outputPath") String outputPath,
    		 						  @DefaultValue("false") @QueryParam("compress") boolean compress,	@DefaultValue("zip") @QueryParam("format") String format,
    		 						  @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                               
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getJobOutputDownload", 
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
       
       // ------------------------- Retrieve Job -----------------------------
       Job job = null;
       var jobsImpl = JobsImpl.getInstance();
       
       try {
    	   job = jobsImpl.getJobByUuid(jobUuid, threadContext.getOboUser(), threadContext.getOboTenantId(),
        		   JobResourceShare.JOB_OUTPUT.name(), JobTapisPermission.READ.name());
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
       
       if (job == null) {
           String msg = MsgUtils.getMsg("JOBS_JOB_NOT_FOUND", jobUuid, threadContext.getOboTenantId());
           _log.warn(msg);
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), prettyPrint, r)).build();
           
       } else if(!job.isVisible()) {
           String msg = MsgUtils.getMsg("JOBS_JOB_NOT_VISIBLE", jobUuid, threadContext.getOboTenantId());
           _log.warn(msg);
           return Response.status(Status.NOT_FOUND).
                   entity(TapisRestUtils.createErrorResponse(MsgUtils.getMsg("JOBS_JOB_NOT_VISIBLE", jobUuid,threadContext.getOboTenantId()), 
                          prettyPrint)).build();
       }
        
       // ------------------------- Check the Job's status -----------------------------
       // If job is still running and not in terminal state then output download cannot be performed.
       
       if(!job.getStatus().isTerminal()) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
    	   return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                   MsgUtils.getMsg("JOBS_JOB_NOT_TERMINATED", jobUuid,threadContext.getOboTenantId(),threadContext.getOboUser(),job.getStatus()), prettyPrint, r)).build(); 
       }
       
      // --------------------------- Check if the the path is a file or Directory ---------------
       // Do a files listing and check FileInfo
       List<FileInfo> filesList = null;
       
       try {
		filesList = jobsImpl.getJobOutputList(job, threadContext.getOboTenantId(), threadContext.getOboUser(), outputPath,
				DEFAULT_LIMIT,DEFAULT_SKIP, JobResourceShare.JOB_OUTPUT.name(), JobTapisPermission.READ.name());
	   } catch (TapisImplException e) {
		   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	   }
       
       String contentDisposition;
       if(filesList == null) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job Output Files List", jobUuid), prettyPrint, r)).build();
       } else {
    	   
    	   // compare outputPath with filesList.get(0).getPath()
    	   // For directory with single file this path will be different in the suffix.
    	   // For example, listing on test/ directory returns files on the test directory with their file name
    	   // appended after the test/ while the outputPath does not have those file names in the path.
    	   //case I : Empty directory
    	   if(filesList.size()== 0){
    		   ResultName missingName = new ResultName();
               missingName.name = jobUuid;
               RespName r = new RespName(missingName);
    		   return Response.status(Status.OK).
    				   entity(TapisRestUtils.createSuccessResponse(
    		                   MsgUtils.getMsg("JOBS_EMPTY_DIR_FOR_DOWNLOAD", jobUuid, outputPath,
    		                		   threadContext.getOboUser(),threadContext.getOboTenantId()), prettyPrint, r)).build();
    		   }
    	   // Case II : Directory with one file vs single file
    	   else if(filesList.size()==1) {
    		   _log.debug("file type =  " + filesList.get(0).getType());
        	   _log.debug("file path =  " + filesList.get(0).getPath());
    		  // get the path for comparison 
    		  String pathInFileInfo = filesList.get(0).getPath();
    		  
    		  // directory with single file
    		  if((filesList.get(0).getType().equals("dir") || !pathInFileInfo.endsWith(outputPath))
    			 || (filesList.get(0).getType().equals("file") && filesList.get(0).getSize()> ONE_GB_IN_KB )	  ) {
    		   compress = true;
    		  }
    	     // for single file, default is false
    	  }
    	   else {
    		   compress = true;;
    	   
           }
       }  
       _log.debug("compress value =  " + compress);
       
       // ------------------------- Locate the output download path and download the file/zipped folder --------------------------
       String mtype = MediaType.APPLICATION_OCTET_STREAM;
      
       JobOutputInfo jobOutputFilesinfo = null;
	   try {
		   jobOutputFilesinfo = jobsImpl.getJobOutputDownloadInfo(job, threadContext.getOboTenantId(), threadContext.getOboUser(), 
				  outputPath);
	   } catch (TapisImplException e) {
		   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	   }
	   
	   boolean isSharedAppCtx = jobsImpl.checkSharedAppCtx(job, jobOutputFilesinfo);
       
       boolean skipTapisAuthorization = false;
	   try {
			skipTapisAuthorization = jobsImpl.isJobShared(job.getUuid(), threadContext.getOboUser(), threadContext.getOboTenantId(), 
					   JobResourceShare.JOB_OUTPUT.name(), JobTapisPermission.READ.name()) || isSharedAppCtx ;
	   } catch (TapisImplException e) {
		   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	   }
	   String sharedAppCtx = "";
       if (isSharedAppCtx == true) {
       	sharedAppCtx = job.getSharedAppCtx();
       }
	   
	   String impersonationId = null;
	   if(skipTapisAuthorization == true) {
		   impersonationId = job.getOwner();
	   }
	   
       DataLocator dataLocator = new DataLocator(job);
       try {
    	  if(jobOutputFilesinfo != null) {
    		   StreamedFile streamFromFiles = dataLocator.getJobOutputDownload(jobOutputFilesinfo, threadContext.getOboTenantId(), 
    				   threadContext.getOboUser(), compress, impersonationId,sharedAppCtx );
    	       contentDisposition = String.format("attachment; filename=%s", streamFromFiles.getName() );
    	       Response response =  Response
	               .ok(streamFromFiles.getInputStream(), mtype)
	               .header("content-disposition",contentDisposition)
	               .header("cache-control", "max-age=3600")
	               .build();
    	       return response;
    		    } 
       } catch (TapisImplException e) {
    	   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
       }
       
       
       return Response.status(Status.NOT_FOUND).
               entity(TapisRestUtils.createErrorResponse(MsgUtils.getMsg("JOBS_NO_OUTPUT_FILES_TO_DOWNLOAD", jobUuid,threadContext.getOboTenantId()), 
                      prettyPrint)).build();
	}
     
    
}

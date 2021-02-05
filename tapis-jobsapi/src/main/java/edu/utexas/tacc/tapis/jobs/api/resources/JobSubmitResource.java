package edu.utexas.tacc.tapis.jobs.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.model.SubmitContext;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.responses.RespSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.dao.JobResubmitDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobResubmit;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.messages.JobSubmitMsg;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@Path("/")
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
     @Path("/submit")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Submit a job for execution.  "
                           + "The main phases of job execution are:\n\n"
            		       + ""
            		       + "  - validate input\n"
            		       + "  - check resource availability\n"
            		       + "  - stage input files\n"
            		       + "  - stage application code\n"
            		       + "  - launch application\n"
            		       + "  - monitor application\n"
            		       + "  - archive application output\n"
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob.class))),
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job created.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespSubmitJob.class))),
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
     public Response submitJob(@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                               InputStream payloadStream)
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "submitJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // The shared code takes it from here.
       return doSubmit(prettyPrint, payloadStream);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* resubmitJob:                                                                 */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobuuid}/resubmit")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Resubmit a job for execution using the original parameters.  "
                           + "The main phases of job execution are:\n\n"
                           + ""
                           + "  - validate input\n"
                           + "  - check resource availability\n"
                           + "  - stage input files\n"
                           + "  - stage application code\n"
                           + "  - launch application\n"
                           + "  - monitor application\n"
                           + "  - archive application output\n"
                           + "",
             tags = "jobs",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job created.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespSubmitJob.class))),
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
     public Response resubmitJob(@PathParam("jobuuid") String jobUuid,
                                 @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
    	 // Trace this request.
    	 if (_log.isTraceEnabled()) {
    		 String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hideJob",
    				 				      "  " + _request.getRequestURL());
    		 _log.trace(msg);
    	 }
     
       // ------------------------- Validate Parameter -----------------------
       if (StringUtils.isAllBlank(jobUuid)) {
         String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "resubmit", "jobuuid");
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).
                 entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Get Resubmit -----------------------------
       // We have a job to resubmit now go lookup the stored job definition
       JobResubmit jobResubmit = null;
       try {
           var jobResubmitDao = new JobResubmitDao();
           jobResubmit = jobResubmitDao.getJobResubmitByUUID(jobUuid);
       } catch (Exception e) {
           String msg = MsgUtils.getMsg("JOBS_JOBRESUBMIT_NOT_FOUND", jobUuid, e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).
                 entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // Make sure we got something.
       if (jobResubmit == null) {
           String msg = MsgUtils.getMsg("JOBS_JOBRESUBMIT_NOT_FOUND", jobUuid, "unknown job uuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // The shared code takes it from here.
       return doSubmit(prettyPrint, jobResubmit.getJobDefinition());
     }
     
     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* doSubmit:                                                                    */
     /* ---------------------------------------------------------------------------- */
     /** Dump the payload from the input stream into a string and then call the 
      * real doSubmit method.
      * 
      * @param prettyPrint the request's query parameter
      * @param payload the request's payload
      * @return the response to the user
      */
     private Response doSubmit(boolean prettyPrint, InputStream payloadStream)
     {
         // ------------------------- Validate Payload -------------------------
         // Read the payload into a string.
         String json = null;
         try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
           catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job submission", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
           }
         
         // The real submit.
         return doSubmit(prettyPrint, json);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* doSubmit:                                                                    */
     /* ---------------------------------------------------------------------------- */
     /** All the work gets done here from both submit and resubmit.
      * 
      * @param prettyPrint the request's query parameter
      * @param payload the request's payload as json
      * @return the response to the user
      */
     private Response doSubmit(boolean prettyPrint, String json)
     {
         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqSubmitJob payload = null;
         try {payload = getPayload(json, FILE_JOB_SUBMIT_REQUEST, ReqSubmitJob.class);} 
         catch (Exception e) {
             String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                          "sbumitJob", e.getMessage());
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
         
         // Create the request context object.
         var reqCtx = new SubmitContext(payload);
         
         // ------------------------- Initialize the Job -----------------------
         // Initialize job with calculated effective parameters.
         Job job = null;
         try {job = reqCtx.initNewJob();}
         catch (TapisImplException e) {
             _log.error(e.getMessage(), e);
             return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }
         catch (Exception e) {
             // This should never happen, but we defend against it. 
             _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }
         
         // ------------------- Create User Subscriptions ----------------------
         // Subscribe to Notifications service on behalf of user.  The complete list
         // of subscriptions are guaranteed by context initialization to have been
         // calculated and non-null by this point. Subscriptions are created before
         // we make any database changes so the caller can access any events generated.  
         if (!reqCtx.getSubmitReq().getSubscriptions().isEmpty()) {
             // TODO: Subscribe to Notifications.
         }
         
         // ------------------------- Save Job ---------------------------------
         // Write the job to the database.
         try {
             var jobsDao = new JobsDao();
             jobsDao.createJob(job);
         }
         catch (Exception e) {
             _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }
         
         // -------------------------- Queue Request ---------------------------
         // Submit the job to the worker queue. Exceptions are mapped to HTTP error codes.
         try {JobQueueManager.getInstance().queueJob(job);}
           catch (Exception e) {
               // Log the error.
               String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", job.getName(), job.getAppId(), e.getMessage());
               _log.error(msg, e);
               
               // Fail the job.  
               failJob(job, msg);
               
               // Let the user know the job failed.
               return Response.status(Status.INTERNAL_SERVER_ERROR).
                       entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
           }
         
         // ------------------------- Save Resubmit Info -----------------------
         // Save the valid job json definition for resubmission in the future
         // table is indexed on id & uuid.  If the actual job submission below
         // fails after this database insertion succeeds, we will have a resubmit
         // record that can never be referenced--no big deal.
         try {
             // Create the resubmit object.
             JobResubmit jobResubmit = new JobResubmit();
             jobResubmit.setJobUuid(job.getUuid());
             jobResubmit.setJobDefinition(json);
             
             // persist job definition json to resubmit table
             var jobResubmitDao = new JobResubmitDao();
             jobResubmitDao.createJobResubmit(jobResubmit);
         } catch (Exception e) {
             String msg = MsgUtils.getMsg("JOBS_JOBRESUBMIT_FAILED_PERSIST", "resubmit", e.getMessage());
             _log.error(msg);
         }
       
         // Success.
         RespSubmitJob r = new RespSubmitJob(job);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("JOBS_CREATED", job.getUuid()), prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* failJob:                                                                     */
     /* ---------------------------------------------------------------------------- */
     /** Mark the job as failed in the database.
      * 
      * @param jobDao the db access object
      * @param job the failed job
      * @param failMsg the failure message
     */
     private static void failJob(Job job, String failMsg)
     {
         // Fail the job.  Note that current status used in the transition 
         // to FAILED is the status of the job as defined in the db.
         try {
             var jobsDao = new JobsDao();
             jobsDao.failJob("submitJob", job, failMsg);
         }
         catch (Exception e) {
             // Swallow exception.
             String msg = MsgUtils.getMsg("JOBS_ZOMBIE_ERROR", 
                                          job.getUuid(), job.getTenant(), "submitJob");
             _log.error(msg, e);
                 
             // Try to send the zombie email.
             sendZombieEmail(job, msg);
         }
     }
       
     /* ---------------------------------------------------------------------------- */
     /* sendZombieEmail:                                                             */
     /* ---------------------------------------------------------------------------- */
     /** Send an email to alert support that a zombie job exists.
      * 
      * @param job the job whose status update failed
      * @param zombiMsg failure message
      */
     private static void sendZombieEmail(Job job, String zombiMsg)
     {
         String subject = "Zombie Job Alert: " + job.getUuid() + " is in a zombie state.";
         try {
               RuntimeParameters runtime = RuntimeParameters.getInstance();
               EmailClient client = EmailClientFactory.getClient(runtime);
               client.send(runtime.getSupportName(),
                       runtime.getSupportEmail(),
                       subject,
                       zombiMsg, HTMLizer.htmlize(zombiMsg));
         }
         catch (Exception e1) {
               // log msg that we tried to send email notice to support.
               RuntimeParameters runtime = RuntimeParameters.getInstance();
               String recipient = runtime == null ? "unknown" : runtime.getSupportEmail();
               String msg = MsgUtils.getMsg("ALOE_SUPPORT_EMAIL_ERROR", recipient, subject, e1.getMessage());
               _log.error(msg, e1);
         }
     }
}

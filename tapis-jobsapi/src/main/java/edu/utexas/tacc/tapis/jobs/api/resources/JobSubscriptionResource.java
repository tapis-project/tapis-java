package edu.utexas.tacc.tapis.jobs.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;

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

import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe;
import edu.utexas.tacc.tapis.jobs.api.responses.RespGetSubscriptions;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.notifications.client.gen.model.RespSubscriptions;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@Path("/")
public class JobSubscriptionResource 
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobSubscriptionResource.class);
    
    // The wildcard used in notifications subject filters.
    private static final String TYPE_FILTER_WILDCARD = JobsApiUtils.TYPE_FILTER_WILDCARD;
    
    // Test for job uuid.
    private static final String JOB_UUID_SUFFIX = "-" + UUIDType.JOB.getCode();
    
    // Json schema resource files.
    private static final String FILE_JOB_SUBCRIBE_REQUEST = 
        "/edu/utexas/tacc/tapis/jobs/api/jsonschema/JobSubscribeRequest.json";
    
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
     /* subscribe:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/subscribe/{jobUuid}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Subcribe to a running job identified by it's UUID. "
                           + "The caller must be the job owner or a tenant administrator.\n\n"
                           + ""
                           + "Like all Job subscription APIs, modifications only "
                           + "affect running jobs and never change the saved job "
                           + "definition. As a consequence, job resubmissions are not "
                           + "affected by runtime subscription changes.\n\n"
                           + ""
                           + "The events to which one can subscribe are:\n\n"
                           + ""
                           + "- JOB_NEW_STATUS - the job has transitioned to a new status\n"
                           + "- JOB_INPUT_TRANSACTION_ID - a request to stage job input files has been submitted\n"
                           + "- JOB_ARCHIVE_TRANSACTION_ID - a request to archive job output files has been submitted\n"
                           + "- JOB_SUBSCRIPTION - a change to the job's subscriptions has occurred\n"
                           + "- JOB_SHARE_EVENT - a job resource has been shared or unshared\n"
                           + "- JOB_ERROR_MESSAGE - the job experienced an error\n"
                           + "- ALL - all job event categories\n"
                           + "",
             tags = "subscriptions",
             security = {@SecurityRequirement(name = "TapisJWT")},
             requestBody = 
                 @RequestBody(
                     required = true,
                     content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe.class))),
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job subscription created.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl.class))),
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
     public Response subscribe(@PathParam("jobUuid") String jobUuid,
                               @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint,
                               InputStream payloadStream)
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "subscribe", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // Eliminate whitespace only input.
       if (StringUtils.isBlank(jobUuid)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "subscribe", "jobUuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Validate Payload -------------------------
       // Read the payload into a string.
       String json = null;
       try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
         catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job subscription", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
       
       // ------------------------- Input Processing -------------------------
       // Parse and validate the json in the request payload, which must exist.
       ReqSubscribe payload = null;
       try {payload = getPayload(json, FILE_JOB_SUBCRIBE_REQUEST, ReqSubscribe.class);} 
       catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", 
                                        "subscribe", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Get Job Info -----------------------------
       // Get the job DTO or return with an error response.
       Object obj = getJobDTO(jobUuid, prettyPrint);
       if (obj instanceof Response) return (Response) obj;
       
       // Get extended job status information.
       JobStatusDTO dto = (JobStatusDTO) obj;
       
       // Is the job still active?
       if (dto.getStatus().isTerminal()) {
           String msg = MsgUtils.getMsg("JOBS_IN_TERMINAL_STATE", jobUuid, dto.getStatus().name());
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
       
       // ------------------------- Check Authz ------------------------------
       // Authorize the user.  Job tenant and oboTenant are guaranteed to match.
       var oboUser = threadContext.getOboUser();
       var oboTenant = threadContext.getOboTenantId();
       var response = checkAuthorization(oboTenant, oboUser, jobUuid, dto.getOwner(), prettyPrint);
       if (response != null) return response;
       
       // ------------------------- Call Notifications -----------------------
       // Marshal the request parameters and create a new subscription in Notifications.
       String url = null;
       try {url = JobsApiUtils.postSubscriptionRequest(payload, oboUser, oboTenant, jobUuid);}
       catch (Exception e) {
           String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", jobUuid, oboUser, oboTenant,
                                        e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }

       // Success.
       RespResourceUrl r = new RespResourceUrl(new ResultResourceUrl());
       r.result.url = url;
       var typeFilter = JobsApiUtils.getNotifTypeFilter(payload.getEventCategoryFilter(), TYPE_FILTER_WILDCARD);
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("NOTIFICATIONS_SUBSCRIPTION_CREATED", jobUuid, typeFilter), 
                  prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* getSubscriptions:                                                            */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/subscribe/{jobUuid}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Retrieve a job's subscriptions fom the Notifications service. "
                           + "After subscriptions expire or are deleted by user action they "
                           + "may no longer be listed in Notification service. To inspect "
                           + "the initial set of subscriptions assigned to a job, retrieve "
                           + "the job definition."
                           + "",
             tags = "subscriptions",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job created.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespGetSubscriptions.class))),
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
     public Response getSubscriptions(@PathParam("jobUuid") String jobUuid,
                                      @DefaultValue("100")   @QueryParam("limit") int limit, 
                                      @DefaultValue("0")     @QueryParam("skip")  int skip,
                                      @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getSubscriptions", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // ------------------------- Get Job Info -----------------------------
       // Get the job DTO or return with an error response.
       Object obj = getJobDTO(jobUuid, prettyPrint);
       if (obj instanceof Response) return (Response) obj;
       
       // Get extended job status information.
       JobStatusDTO dto = (JobStatusDTO) obj;

       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
           var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
           _log.error(msg);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }
       
       // ------------------------- Check Authz ------------------------------
       // Authorize the user.  Job tenant and oboTenant are guaranteed to match.
       var oboUser = threadContext.getOboUser();
       var oboTenant = threadContext.getOboTenantId();
       var response = checkAuthorization(oboTenant, oboUser, jobUuid, dto.getOwner(), prettyPrint);
       if (response != null) return response;
       
       // ------------------------- Call Notifications -----------------------
       // Marshal the request parameters and create a new subscription in Notifications.
       RespSubscriptions resp = null;
       try {
           var jobsImpl = JobsImpl.getInstance(); 
           resp = jobsImpl.getSubscriptions(jobUuid, limit, skip, oboUser, oboTenant);
       }
       catch (Exception e) {
           String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", jobUuid, oboUser, oboTenant,
                                        e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
       }

       // Success.
       var r = new RespGetSubscriptions(resp);
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("JOBS_SUBSCRIPTIONS_RETRIEVED", jobUuid, r.result.size()),
                  prettyPrint, r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* deleteSubscribe:                                                             */
     /* ---------------------------------------------------------------------------- */
     @DELETE
     @Path("/subscribe/{uuid}")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     @Operation(
             description = "Depending on the UUID provide, this API either deletes a "
                           + "single subscription from a job or all subscriptions "
                           + "from a job. To delete single subscription, provide the UUID "
                           + "of that subscription as listed in the subscription retrieval "
                           + "result for the job.  To delete all a job's subscriptions, specify "
                           + "the job UUID.\n\n"
                           + ""
                           + "Like all Job subscription APIs, modifications only "
                           + "affect running jobs and never change the saved job "
                           + "definition. As a consequence, job resubmissions are not "
                           + "affected by runtime subscription changes."
                           + "",
             tags = "subscriptions",
             security = {@SecurityRequirement(name = "TapisJWT")},
             responses = 
                 {
                  @ApiResponse(responseCode = "200", description = "Job created.",
                      content = @Content(schema = @Schema(
                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount.class))),
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
     public Response deleteSubscriptions(@PathParam("uuid") String uuid,
                                         @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "deleteSubscriptions", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // Determine which type of deletion takes place based on whether a job uuid is passed in.
       if (uuid.endsWith(JOB_UUID_SUFFIX)) return deleteJobSubscriptions(uuid, prettyPrint);
         else return deleteJobSubscription(uuid, prettyPrint);
     }
     
     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* getJobDTO:                                                                   */
     /* ---------------------------------------------------------------------------- */
     /** Return the job's extended status information or a Response object when there's
      * an error.
      * 
      * @param jobUuid the job uuid
      * @param prettyPrint whether to pretty print on error conditions  
      * @return a JobStatusDTO (success) or a Response (error)
      */
     private Object getJobDTO(String jobUuid, boolean prettyPrint)
     {
         // Get extended job status information.
         JobStatusDTO dto = null;
         try {
             var jobsDao = new JobsDao();
             dto = jobsDao.getJobStatusByUUID(jobUuid);
         }
         catch (Exception e) {
             _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }
         
         // Did we find the job?
         if (dto == null) {
             String msg = MsgUtils.getMsg("JOBS_JOB_NOT_FOUND", jobUuid);
             _log.error(msg);
             return Response.status(Status.BAD_REQUEST).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Success.
         return dto;
     }
     
     /* ---------------------------------------------------------------------------- */
     /* checkAuthorization:                                                          */
     /* ---------------------------------------------------------------------------- */
     /** Restrict the requestor to job owner or tenant administrator.
      * 
      * @return null if authorization passed, Response on failure
      */
     private Response checkAuthorization(String oboTenant, String oboUser, 
                                         String jobUuid, String jobOwner, boolean prettyPrint)
     {
         try {
             // Only job owners and tenant admins can subscribe to a job.
             if (!oboUser.equals(jobOwner) && !TapisUtils.isAdmin(oboUser, oboTenant)) {
                 var msg = MsgUtils.getMsg("JOBS_JOB_ACTION_NOT_AUTHORIZED", oboUser, 
                                           "subscription", jobUuid);
                 _log.error(msg);
                 return Response.status(Status.UNAUTHORIZED).
                         entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
         }
         catch (Exception e) {
             _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
         }

         // Authorization succeeded.
         return null;
     }
     
     /* ---------------------------------------------------------------------------- */
     /* checkSubscriptionTenant:                                                     */
     /* ---------------------------------------------------------------------------- */
     private Response checkSubscriptionTenant(String subTenant, String oboTenant, 
                                              String jobTenant, boolean prettyPrint)
     {
         // Make sure the subscription is in the same tenant in which the user is
         // authenticated and the job resides in.
         if (!oboTenant.equals(subTenant) || !jobTenant.equals(subTenant)) {
             var msg = MsgUtils.getMsg("JOBS_MISMATCHED_SUBSCRIPTION_TENANT", subTenant, jobTenant); 
             _log.error(msg);
             return Response.status(Status.UNAUTHORIZED).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Authorization succeeded.
         return null;
     }
     
     /* ---------------------------------------------------------------------------- */
     /* deleteJobSubscriptions:                                                      */
     /* ---------------------------------------------------------------------------- */
     private Response deleteJobSubscriptions(String jobUuid, boolean prettyPrint)
     {
         // ------------------------- Get Job Info -----------------------------
         // Get the job DTO or return with an error response.
         Object obj = getJobDTO(jobUuid, prettyPrint);
         if (obj instanceof Response) return (Response) obj;
         
         // Get extended job status information.
         JobStatusDTO dto = (JobStatusDTO) obj;
         
         // ------------------------- Create Context ---------------------------
         // Validate the threadlocal content here so no subsequent code on this request needs to.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         if (!threadContext.validate()) {
             var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
             _log.error(msg);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Check Authz ------------------------------
         // Authorize the user.  Job tenant and oboTenant are guaranteed to match.
         var oboUser = threadContext.getOboUser();
         var oboTenant = threadContext.getOboTenantId();
         var response = checkAuthorization(oboTenant, oboUser, jobUuid, dto.getOwner(), prettyPrint);
         if (response != null) return response;
         
         // ------------------------- Call Notifications -----------------------
         // Delete all subscriptions to a specific job.
         int deleted = 0;
         try {
             var jobsImpl = JobsImpl.getInstance(); 
             deleted = jobsImpl.deleteJobSubscriptions(jobUuid, oboUser, oboTenant);
         }
         catch (Exception e) {
             String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", jobUuid, oboUser, oboTenant,
                                          e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Success.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = deleted;
         RespChangeCount r = new RespChangeCount(count);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "subscriptions", jobUuid),
                    prettyPrint, r)).build();
     }

     /* ---------------------------------------------------------------------------- */
     /* deleteJobSubscription:                                                       */
     /* ---------------------------------------------------------------------------- */
     private Response deleteJobSubscription(String uuid, boolean prettyPrint)
     {
         // ------------------------- Create Context ---------------------------
         // Validate the threadlocal content here so no subsequent code on this request needs to.
         TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
         if (!threadContext.validate()) {
             var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
             _log.error(msg);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // Get obo info.
         var oboUser = threadContext.getOboUser();
         var oboTenant = threadContext.getOboTenantId();
         
         // ------------------------- Get Subscription -------------------------
         // We need to get the owner and subject from the subscription.
         String subSubject = null;
         String subOwner   = null;
         String subTenant  = null;
         try {
             // Retrieve the subscription.
             var jobsImpl = JobsImpl.getInstance(); 
             var subscription = jobsImpl.getSubscriptionByUUID(uuid, oboUser, oboTenant);
             
             // Did we find a subscription?
             if (subscription == null) {
                 String msg = MsgUtils.getMsg("TAPIS_NOT_FOUND", "subscription", uuid);
                 _log.error(msg);
                 return Response.status(Status.BAD_REQUEST).
                         entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
             }
             
             // Extract subscription information.
             subSubject = subscription.getSubjectFilter();
             subOwner   = subscription.getOwner();
             subTenant  = subscription.getTenant();
         }
         catch (Exception e) {
             var jobref = subSubject == null ? "unknown" : subSubject;
             String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", jobref, oboUser, oboTenant,  
                                          e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }
         
         // ------------------------- Get Job Info -----------------------------
         // Get the job DTO or return with an error response.
         Object obj = getJobDTO(subSubject, prettyPrint);
         if (obj instanceof Response) return (Response) obj;
         
         // Get extended job status information.
         JobStatusDTO dto = (JobStatusDTO) obj;

         // ------------------------- Check Authz ------------------------------
         // Authorize the user.  Job tenant and oboTenant are guaranteed to match.
         // Note that the subscription owner may be different the job owner or tenant
         // admin, but we still want the subscription to be deleted.  If the oboUser
         // is the subscription owner, we let processing proceed.
         var response = checkAuthorization(oboTenant, oboUser, subSubject, dto.getOwner(), prettyPrint);
         if (response != null && !oboUser.equals(subOwner)) return response;
         
         // Make sure the subscription, job and obo tenants are the same.
         response = checkSubscriptionTenant(subTenant, oboTenant, dto.getTenant(), prettyPrint);
         if (response != null) return response;
         
         // ------------------------- Delete Subscription ----------------------
         // Delete the specific subscription.
         int deleted = 0;
         try {
             var jobsImpl = JobsImpl.getInstance(); 
             deleted = jobsImpl.deleteJobSubscription(uuid, oboUser, oboTenant);
         }
         catch (Exception e) {
             String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", "", oboUser, oboTenant,  // TODO: **
                                          e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
         }

         // Success.
         ResultChangeCount count = new ResultChangeCount();
         count.changes = deleted;
         RespChangeCount r = new RespChangeCount(count);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("TAPIS_DELETED", "subscription", uuid),
                    prettyPrint, r)).build();
     }
}

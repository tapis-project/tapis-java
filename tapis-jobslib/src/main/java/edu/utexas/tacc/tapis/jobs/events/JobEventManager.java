package edu.utexas.tacc.tapis.jobs.events;

import java.sql.Connection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.gen.model.TransferStatusEnum;
import edu.utexas.tacc.tapis.jobs.dao.JobEventsDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;

/** This class records noteworthy job events and asynchronously send notifications
 * to subscribers.  The threads that call a record method synchronously write to the
 * event table but only queue up events for notification transmission.  No remote
 * notification processing should ever take place on a thread calling a record  
 * method since that thread may be in the middle of a database transaction.
 * 
 * @author rcardone
 */
public final class JobEventManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobEventManager.class);
    
    // Extended event messages.
    private static final String OLD_STATUS_ADDENDUM = " The previous job status was ";
    private static final String NEW_ERROR_ADDENDUM = " The error message stack is: ";
    private static final String SUBSCRIPTION_ADDENDUM = " A subscription was ";
    private static final String SUBSCRIPTION_ADDENDUM_MULTI = " One or more subscriptions were ";
    
    // Detail value for job final messages.
    private static final String DEFAULT_FINAL_MSG = "Final job message.";
    
    // DB field limits.
    private static final int MAX_EVENT_DESCRIPTION = 16384;
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    public enum SubscriptionActions {added, removed}
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private final JobEventsDao _jobEventsDao;
    private final JobsDao      _jobsDao;
    
    /* ********************************************************************** */
    /*                       SingletonInitializer class                       */
    /* ********************************************************************** */
    /** Bill Pugh method of singleton initialization. */
    private static final class SingletonInitializer
    {
        private static final JobEventManager _instance = new JobEventManager(); 
    }
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private JobEventManager() 
    {
        try {_jobEventsDao = new JobEventsDao();}
            catch (Exception e) {
                throw new TapisRuntimeException(e.getMessage(), e);
            }
        try {_jobsDao = new JobsDao();}
            catch (Exception e) {
                throw new TapisRuntimeException(e.getMessage(), e);
            }
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    public static JobEventManager getInstance() {return SingletonInitializer._instance;}

    /* ---------------------------------------------------------------------- */
    /* recordStatusEvent:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Write Job status change event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param job the job that generated the event
     * @param newStatus required new status
     * @param oldStatus optional previous status
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordStatusEvent(Job job, JobStatusType newStatus, 
                                      JobStatusType oldStatus, Connection conn)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_NEW_STATUS);
        jobEvent.setJobUuid(job.getUuid());
        jobEvent.setTenant(job.getTenant());
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription() + newStatus.name() + ".";
        if (oldStatus != null) desc += OLD_STATUS_ADDENDUM + oldStatus.name() + ".";
        jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getNewStatusEventDetail(job, newStatus, oldStatus);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, conn);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }

    /* ---------------------------------------------------------------------- */
    /* recordStagingInputsEvent:                                              */
    /* ---------------------------------------------------------------------- */
    /** Write Job staging input event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param job the job that generated the event
     * @param transferStatus status of transfer as reported by Files
     * @param transactionId transaction id from the Files service
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordStagingInputsEvent(Job job, TransferStatusEnum transferStatus, 
                                             String transactionId)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_INPUT_TRANSACTION_ID);
        jobEvent.setJobUuid(job.getUuid());
        jobEvent.setTenant(job.getTenant());
        jobEvent.setOthUuid(transactionId);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += " Files service staging transaction " + transactionId + " in state " +
                transferStatus.name() + " for job " + job.getUuid() + 
                " in tenant " + job.getTenant() + ".";
        jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getTransferEventDetail(job, transferStatus,
                                                           transactionId);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }

    /* ---------------------------------------------------------------------- */
    /* recordArchivingEvent:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Write Job archiving event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param job the job that generated the event
     * @param transferStatus status of transfer as reported by Files
     * @param transactionId transaction id from the Files service
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordArchivingEvent(Job job, TransferStatusEnum transferStatus, 
                                         String transactionId)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_ARCHIVE_TRANSACTION_ID);
        jobEvent.setJobUuid(job.getUuid());
        jobEvent.setTenant(job.getTenant());
        jobEvent.setOthUuid(transactionId);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += " Files service archiving transaction " + transactionId + " in state " +
                transferStatus.name() + " for job " + job.getUuid() + 
                " in tenant " + job.getTenant() + ".";
        jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getTransferEventDetail(job, transferStatus,
                                                           transactionId);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordShareEvent:                                                      */
    /* ---------------------------------------------------------------------- */
    public JobEvent recordShareEvent(String jobUuid, String tenant, String resourceType, 
                                     String shareType, String grantee, String grantor)	
     throws TapisException
    {
		// Create the Job event.
		var jobEvent = new JobEvent();
		jobEvent.setEvent(JobEventType.JOB_SHARE_EVENT);
		jobEvent.setJobUuid(jobUuid);
		jobEvent.setTenant(tenant);
		
		// Can we augment the standard event description?
		var desc = jobEvent.getEvent().getDescription();
		desc += " Grantor " + grantor + " shared job resource " + resourceType + " with grantee " + grantee + ".";
		jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
		// shareType example: "SHARE_JOB_HISTORY_READ"
        var detail = JobEventDetail.getShareEventDetail(jobUuid, tenant, resourceType,
                                                        shareType, grantee, grantor, _jobsDao);
        jobEvent.setEventDetail(detail);
		
		// Save in db.
		_jobEventsDao.createEvent(jobEvent, null);
		postEventToNotificationService(jobEvent);
		return jobEvent;
    }
  
   /* ---------------------------------------------------------------------- */
   /* recordUnShareEvent:                                                    */
   /* ---------------------------------------------------------------------- */
   public JobEvent recordUnShareEvent(JobShared js, String shareType)
		   throws TapisException
   {
		// Create the Job event.
		var jobEvent = new JobEvent();
		jobEvent.setEvent(JobEventType.JOB_SHARE_EVENT);
		jobEvent.setJobUuid(js.getJobUuid());
		jobEvent.setTenant(js.getTenant());
		
		// Can we augment the standard event description?
		var desc = jobEvent.getEvent().getDescription();
		desc += " Grantor " + js.getGrantor() + " unshared job resource " 
		        + js.getJobResource().name() + " with grantee " + js.getGrantee() + ".";
		jobEvent.setDescription(desc);
		
        // Fill in the event details as a JSON object.
        // shareType example: "UNSHARE_ResourceType_Priviledge"
        var detail = JobEventDetail.getShareEventDetail(js.getJobUuid(), js.getTenant(), 
                js.getJobResource().name(), shareType, js.getGrantee(), js.getGrantor(), _jobsDao);
        jobEvent.setEventDetail(detail);
        
		// Save in db.
		_jobEventsDao.createEvent(jobEvent, null);
		postEventToNotificationService(jobEvent);
		return jobEvent;
   }
   
   /* ---------------------------------------------------------------------- */
   /* recordErrorEvent:                                                      */
   /* ---------------------------------------------------------------------- */
   /** Write Job significant error event to database.  
    * 
    * @param jobUuid the job that generated the event
    * @param tenant the job tenant
    * @param status current job status
    * @param message the event message
    * @throws TapisException on error
    */
   public JobEvent recordErrorEvent(String jobUuid, String tenant, JobStatusType status, 
                                    String message) 
     throws TapisException
   {
       // Create the Job event.
       var jobEvent = new JobEvent();
       jobEvent.setEvent(JobEventType.JOB_ERROR_MESSAGE);
       jobEvent.setJobUuid(jobUuid);
       jobEvent.setTenant(tenant);
       
       // Truncate messages that are too long.
       if (message.length() > MAX_EVENT_DESCRIPTION) 
           message = message.substring(0, MAX_EVENT_DESCRIPTION);
       jobEvent.setDescription(message);
       
       // Fill in the event details as a JSON object.
       var detail = JobEventDetail.getErrorEventDetail(jobUuid, status, _jobsDao);
       jobEvent.setEventDetail(detail);
       
       // Save in db and send to notifications service asynchronously.
       _jobEventsDao.createEvent(jobEvent, null);
       postEventToNotificationService(jobEvent);
       return jobEvent;
   }
   
    /* ---------------------------------------------------------------------- */
    /* recordErrorEvent:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Write Job significant error event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param jobUuid the job that generated the event
     * @param tenant the job tenant
     * @param status current job status
     * @param messages this of messages from stack trace
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordErrorEvent(String jobUuid, String tenant, JobStatusType status,
                                     List<String> messages, Connection conn) 
      throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_ERROR_MESSAGE);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        StringBuilder stackMessages = new StringBuilder();
        stackMessages.append("[");
        if (!messages.isEmpty()) {
            int i = 1;
            for (var msg : messages) {
                if (i != 1) stackMessages.append(" ");
                stackMessages.append(i++);
                stackMessages.append(". ");
                stackMessages.append(msg);
            }
        }
        stackMessages.append("]");
        desc += NEW_ERROR_ADDENDUM + stackMessages.toString();
        
        // Truncate messages that are too long.
        if (desc.length() > MAX_EVENT_DESCRIPTION) 
            desc = desc.substring(0, MAX_EVENT_DESCRIPTION);
        jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getErrorEventDetail(jobUuid, status, _jobsDao);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, conn);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordSubscriptionEvent:                                               */
    /* ---------------------------------------------------------------------- */
    /** Write Job subscription event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param jobUuid the job that generated the event
     * @param tenant the job tenant
     * @param action added or removed
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordSubscriptionEvent(String jobUuid, String tenant, 
                                            SubscriptionActions action,
                                            int numSubscriptions)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_SUBSCRIPTION);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        jobEvent.setEventDetail(action.name());
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        if (action == SubscriptionActions.added)  
            desc += SUBSCRIPTION_ADDENDUM + action.name() + ".";
          else desc += SUBSCRIPTION_ADDENDUM_MULTI + action.name() + ".";
        jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getSubscriptionEventDetail(jobUuid, tenant, 
                            SubscriptionActions.added, numSubscriptions, _jobsDao);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }

    /* ---------------------------------------------------------------------- */
    /* recordJobSubmitSubscriptionsEvent:                                     */
    /* ---------------------------------------------------------------------- */
    /** Write Job subscription event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param job the job that generated the event
     * @throws TapisException on error
     */
    public JobEvent recordJobSubmitSubscriptionsEvent(Job job, int numSubscriptions)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_SUBSCRIPTION);
        jobEvent.setJobUuid(job.getUuid());
        jobEvent.setTenant(job.getTenant());
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += "  " + numSubscriptions + " subscription(s) created with job submission.";
        jobEvent.setDescription(desc);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getSubmitSubscriptionEventDetail(
                       job, SubscriptionActions.added, numSubscriptions);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }

    /* ---------------------------------------------------------------------- */
    /* recordFinalMessageEvent:                                               */
    /* ---------------------------------------------------------------------- */
    /** Write Job subscription event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction. 
     * 
     * @param job the job that generated the event
     * @param finalMsg the job's final message
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordFinalMessageEvent(Job job, String finalMsg)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_ERROR_MESSAGE);
        jobEvent.setJobUuid(job.getUuid());
        jobEvent.setTenant(job.getTenant());
        
        // Always assign description.
        if (StringUtils.isBlank(finalMsg)) finalMsg = DEFAULT_FINAL_MSG;
        if (finalMsg.length() > MAX_EVENT_DESCRIPTION)
            finalMsg = finalMsg.substring(0, MAX_EVENT_DESCRIPTION);
        jobEvent.setDescription(finalMsg);
        
        // Fill in the event details as a JSON object.
        var detail = JobEventDetail.getFinalEventDetail(job);
        jobEvent.setEventDetail(detail);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }

    /* ---------------------------------------------------------------------- */
    /* recordUserEvent:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Write a User event to database.  
     * 
     * The connection parameter can be null if the event insertion is not to 
     * be part of an in-progress transaction.  The eventDetail length must
     * have been verified by the caller to be between 1 and MAX_EVENT_DETAIL. 
     * 
     * @param jobUuid the job targeted by the event
     * @param tenant the job tenant
     * @param sender the tapis user that sent the event
     * @param eventDetail the user provided body of the event
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordUserEvent(String jobUuid, String tenant, String sender,
                                    String eventDetail, Connection conn)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_USER_EVENT);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        jobEvent.setEventDetail(eventDetail);  // length checked by json schema 
        
        // Fill in placeholders in the event description.
        var desc = jobEvent.getEvent().getDescription();
        desc = desc.replace("<user>", sender);
        desc = desc.replace("<uuid>", jobUuid);
        jobEvent.setDescription(desc);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, conn);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* postEventToNotificationService:                                        */
    /* ---------------------------------------------------------------------- */
    /** Best effort attempt to post event to the event reader's queue.
     * 
     * @param jobEvent the event ultimately destined for notifications
     */
    private void postEventToNotificationService(JobEvent jobEvent)
    {
        // Error already logged.
        try {JobQueueManager.getInstance().postEventQueue(jobEvent);}
            catch (Exception e) {}
    }
}

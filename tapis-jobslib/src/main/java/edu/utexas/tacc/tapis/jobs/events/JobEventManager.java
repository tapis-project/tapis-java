package edu.utexas.tacc.tapis.jobs.events;

import java.sql.Connection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.gen.model.TransferStatusEnum;
import edu.utexas.tacc.tapis.jobs.dao.JobEventsDao;
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
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    public enum SubscriptionActions {added, removed}
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private final JobEventsDao _jobEventsDao;
    
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
     * @param jobUuid the job that generated the event
     * @param tenant the job tenant
     * @param newStatus required new status
     * @param oldStatus optional previous status
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordStatusEvent(String jobUuid, String tenant, JobStatusType newStatus, 
                                      JobStatusType oldStatus, Connection conn)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_NEW_STATUS);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        jobEvent.setEventDetail(newStatus.name());
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription() + newStatus.name() + ".";
        if (oldStatus != null) desc += OLD_STATUS_ADDENDUM + oldStatus.name() + ".";
        jobEvent.setDescription(desc);
        
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
     * @param jobUuid the job that generated the event
     * @param tenant the job tenant
     * @param status current job status
     * @param transactionId transaction id from the Files service
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordStagingInputsEvent(String jobUuid, String tenant,  
                                             TransferStatusEnum transferStatus, 
                                             String transactionId)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_INPUT_TRANSACTION_ID);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        jobEvent.setEventDetail(transferStatus.name());
        jobEvent.setOthUuid(transactionId);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += " Files service transaction " + transactionId + " in state " +
                transferStatus.name() + " for job " + jobUuid + " in tenant " + tenant + ".";
        jobEvent.setDescription(desc);
        
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
     * @param jobUuid the job that generated the event
     * @param tenant the job tenant
     * @param status current job status
     * @param transactionId transaction id from the Files service
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordArchivingEvent(String jobUuid, String tenant,  
                                         TransferStatusEnum transferStatus, 
                                         String transactionId)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_ARCHIVE_TRANSACTION_ID);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        jobEvent.setEventDetail(transferStatus.name());
        jobEvent.setOthUuid(transactionId);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += " Files service transaction " + transactionId + " in state " +
                transferStatus.name() + " for job " + jobUuid + " in tenant " + tenant + ".";
        jobEvent.setDescription(desc);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
        postEventToNotificationService(jobEvent);
        return jobEvent;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordShareEvent:                                                      */
    /* ---------------------------------------------------------------------- */
    public JobEvent recordShareEvent(String jobUuid, String tenant, String resourceType, 
                                     String event, String grantee, String grantor)	
     throws TapisException
    {
		// Create the Job event.
		var jobEvent = new JobEvent();
		jobEvent.setEvent(JobEventType.JOB_SHARE_EVENT);
		jobEvent.setJobUuid(jobUuid);
		jobEvent.setTenant(tenant);
		jobEvent.setEventDetail(event); // ex."SHARE_JOB_HISTORY_READ"
		
		// Can we augment the standard event description?
		var desc = jobEvent.getEvent().getDescription();
		desc += " Grantor " + grantor + " shares the job resource " + resourceType + " with grantee " + grantee + ".";
		jobEvent.setDescription(desc);
		
		// Save in db.
		_jobEventsDao.createEvent(jobEvent, null);
		postEventToNotificationService(jobEvent);
		return jobEvent;
    }
  
   /* ---------------------------------------------------------------------- */
   /* recordUnShareEvent:                                                    */
   /* ---------------------------------------------------------------------- */
   public JobEvent recordUnShareEvent(JobShared js, String event)
		   throws TapisException
   {
		// Create the Job event.
		var jobEvent = new JobEvent();
		jobEvent.setEvent(JobEventType.JOB_SHARE_EVENT);
		jobEvent.setJobUuid(js.getJobUuid());
		jobEvent.setTenant(js.getTenant());
		jobEvent.setEventDetail(event);// ex."UNSHARE_ResourceType_Priviledge"
		
		// Can we augment the standard event description?
		var desc = jobEvent.getEvent().getDescription();
		desc += " Grantor " + js.getCreatedby() + " unshares the job  " 
		        + js.getJobUuid() + " resource " + js.getJobResource().name() 
		        + " with grantee " + js.getGrantee() + " in tenant "+ js.getTenant() + ".";
		jobEvent.setDescription(desc);
		
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
        jobEvent.setEventDetail(status.name());
        
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
        jobEvent.setDescription(desc);
        
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
                                            SubscriptionActions action)
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
     * @param jobUuid the job that generated the event
     * @param tenant the job tenant
     * @param action added or removed
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordJobSubmitSubscriptionsEvent(String jobUuid, String tenant,
                                                      int numSubscriptions)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_SUBSCRIPTION);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setTenant(tenant);
        jobEvent.setEventDetail(SubscriptionActions.added.name().toUpperCase());
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += "  " + numSubscriptions + " subscription(s) created with job submission.";
        jobEvent.setDescription(desc);
        
        // Save in db and send to notifications service asynchronously.
        _jobEventsDao.createEvent(jobEvent, null);
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

package edu.utexas.tacc.tapis.jobs.events;

import java.sql.Connection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobEventsDao;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
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
    private static final String STAGING_TRANS_ADDENDUM = " The Files service transaction id is ";
    private static final String NEW_ERROR_ADDENDUM = " The error message stack is: ";
    
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
     * @param newStatus required new status
     * @param oldStatus optional previous status
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordStatusEvent(String jobUuid, JobStatusType newStatus, 
                                      JobStatusType oldStatus, Connection conn)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_NEW_STATUS);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setEventDetail(newStatus.name());
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription() + newStatus.name() + ".";
        if (oldStatus != null) desc += OLD_STATUS_ADDENDUM + oldStatus.name() + ".";
        jobEvent.setDescription(desc);
        
        // Save in db.
        _jobEventsDao.createEvent(jobEvent, conn);
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
     * @param status current job status
     * @param transactionId transaction id from the Files service
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordStagingInputsEvent(String jobUuid, JobStatusType status, 
                                             String transactionId, Connection conn)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_INPUT_TRANSACTION_ID);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setEventDetail(status.name());
        jobEvent.setOthUuid(transactionId);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += STAGING_TRANS_ADDENDUM + transactionId + ".";
        jobEvent.setDescription(desc);
        
        // Save in db.
        _jobEventsDao.createEvent(jobEvent, conn);
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
     * @param status current job status
     * @param transactionId transaction id from the Files service
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordArchivingEvent(String jobUuid, JobStatusType status, 
                                         String transactionId, Connection conn)
     throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_ARCHIVE_TRANSACTION_ID);
        jobEvent.setJobUuid(jobUuid);
        jobEvent.setEventDetail(status.name());
        jobEvent.setOthUuid(transactionId);
        
        // Can we augment the standard event description?
        var desc = jobEvent.getEvent().getDescription();
        desc += STAGING_TRANS_ADDENDUM + transactionId + ".";
        jobEvent.setDescription(desc);
        
        // Save in db.
        _jobEventsDao.createEvent(jobEvent, conn);
        return jobEvent;
    }
    
   public JobEvent recordShareEvent(String jobUuid, String resourceType, String event, String grantee, 
            String grantor)	throws TapisException
   {
		// Create the Job event.
		var jobEvent = new JobEvent();
		jobEvent.setEvent(JobEventType.JOB_SHARE_EVENT);
		jobEvent.setJobUuid(jobUuid);
		jobEvent.setEventDetail(event);// ex."SHARE_JOB_HISTORY_READ"
		//jobEvent.setOthUuid("");
		
		// Can we augment the standard event description?
		var desc = jobEvent.getEvent().getDescription();
		desc += " Grantor " + grantor + " shares the job resource " + resourceType + " with grantee " + grantee + ".";
		jobEvent.setDescription(desc);
		
		// Save in db.
		//_jobEventsDao.createEvent(jobEvent);
		_jobEventsDao.createEvent(jobEvent, null);
		return jobEvent;
    }
  
   public JobEvent recordUnShareEvent(JobShared js, String event)
		   throws TapisException
  {
		// Create the Job event.
		var jobEvent = new JobEvent();
		jobEvent.setEvent(JobEventType.JOB_SHARE_EVENT);
		jobEvent.setJobUuid(js.getJobUuid());
		jobEvent.setEventDetail(event);// ex."UNSHARE_ResourceType_Priviledge"
		//jobEvent.setOthUuid("");
		
		// Can we augment the standard event description?
		var desc = jobEvent.getEvent().getDescription();
		desc += " Grantor " + js.getCreatedby() + " unshares the job  " + js.getJobUuid() + " resource " + js.getJobResource().name() + " with grantee " + js.getGrantee() + " in tenant "+ js.getTenant() +".";
		jobEvent.setDescription(desc);
		
		// Save in db.
		//_jobEventsDao.createEvent(jobEvent);
		_jobEventsDao.createEvent(jobEvent, null);
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
     * @param status current job status
     * @param messages this of messages from stack trace
     * @param conn existing connection or null
     * @throws TapisException on error
     */
    public JobEvent recordErrorEvent(String jobUuid, JobStatusType status, 
                                     List<String> messages, Connection conn) 
      throws TapisException
    {
        // Create the Job event.
        var jobEvent = new JobEvent();
        jobEvent.setEvent(JobEventType.JOB_ERROR_MESSAGE);
        jobEvent.setJobUuid(jobUuid);
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
        
        // Save in db.
        _jobEventsDao.createEvent(jobEvent, conn);
        return jobEvent;
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
}

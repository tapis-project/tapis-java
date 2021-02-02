package edu.utexas.tacc.tapis.jobs.events;

import java.sql.Connection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobEventsDao;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;

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
        jobEvent.setJobStatus(newStatus);
        
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
        jobEvent.setJobStatus(status);
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
        jobEvent.setJobStatus(status);
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
        jobEvent.setJobStatus(status);
        
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

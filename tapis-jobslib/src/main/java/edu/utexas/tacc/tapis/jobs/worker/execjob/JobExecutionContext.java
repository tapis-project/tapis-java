package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobStatusMsg;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobExecutionContext
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobExecutionContext.class);
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The job to run.
    private final Job                _job;
    
	// Cached dao's used throughout this file and by clients.
    private final JobsDao            _jobsDao;
    
    // Last message to be written to job record when job terminates.
    private String                   _finalMessage; 

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobExecutionContext(Job job, JobsDao jobDao) 
    {
        // Jobs and their dao's cannot be null.
        if (job == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "JobExecutionContext", "job");
            throw new TapisRuntimeException(msg);
        }
        if (jobDao == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "JobExecutionContext", "jobDao");
            throw new TapisRuntimeException(msg);
        }
        _job = job;
        _jobsDao = jobDao;
        
        // Cross reference the job and its context.
        job.setJobCtx(this);
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getJob:                                                                */
    /* ---------------------------------------------------------------------- */
    public Job getJob() {return _job;}
    
    /* ---------------------------------------------------------------------- */
    /* getFinalMessage:                                                       */
    /* ---------------------------------------------------------------------- */
    public String getFinalMessage() {return _finalMessage;}

    /* ---------------------------------------------------------------------- */
    /* setFinalMessage:                                                       */
    /* ---------------------------------------------------------------------- */
    public void setFinalMessage(String finalMessage) {
        this._finalMessage = finalMessage;
    }

    /* ********************************************************************** */
    /*                      Asynchronous Command Methods                      */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* checkCmdMsg:                                                           */
    /* ---------------------------------------------------------------------- */
    public void checkCmdMsg()
     throws JobAsyncCmdException
    {
        // See if there the job received a command message
        // and reset the job's message field to null.
        CmdMsg cmdMsg = _job.getAndSetCmdMsg();
        if (cmdMsg == null) return;
        
        // Process each message based on type.  The cancel and paused commands
        // change the job state and throw a JobAsyncCmdException to terminate
        // or postpone job processing.
        switch (cmdMsg.msgType) {
            case JOB_STATUS:  
                JobExecutionUtils.executeCmdMsg(this, (JobStatusMsg) cmdMsg);
                break;
            case JOB_CANCEL: 
                JobExecutionUtils.executeCmdMsg(this, cmdMsg, JobStatusType.CANCELLED);
                break;
            case JOB_PAUSE: 
                JobExecutionUtils.executeCmdMsg(this, cmdMsg, JobStatusType.PAUSED);
                break;
            default:
                // This should not happen.  Log it and move on.
                String msg = MsgUtils.getMsg("JOBS_CMD_MSG_ERROR", _job.getUuid(), cmdMsg.msgType.name(),
                                             cmdMsg.senderId, cmdMsg.correlationId);
                _log.error(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* checkForCancelBeforeRecovery:                                          */
    /* ---------------------------------------------------------------------- */
    /** Check for user cancellation.  This method is intended for use when the
     * job has experienced an error and also received a cancel message.  The
     * cancel message take precedence over any recovery that might be attempted.
     * This method clears the cmdmsg, so it destroys any non-cancel message that 
     * might have been sent. 
     * 
     * @return true if the job was cancelled, false otherwise.
     */
    public boolean checkForCancelBeforeRecovery()
    {
        // See if there the job received a cancel message
        // and reset the job's message field to null.
        CmdMsg cmdMsg = _job.getAndSetCmdMsg();
        if (cmdMsg == null || cmdMsg.msgType != cmdMsg.msgType.JOB_CANCEL) return false; 
        
        // Execute the cancel and indicate it by passing back true.
        try {JobExecutionUtils.executeCmdMsg(this, cmdMsg, JobStatusType.CANCELLED);}
            catch (JobAsyncCmdException e) {
                _log.info(MsgUtils.getMsg("JOBS_CMD_MSG_CANCELLED_BEFORE_RECOVERY", _job.getUuid()));
                return true;
            }
        
        // We didn't get the expected async exception, 
        // so we can't say cancellation succeeded.
        return false;
    }

    /* ---------------------------------------------------------------------- */
    /* getJobsDao:                                                            */
    /* ---------------------------------------------------------------------- */
    public JobsDao getJobsDao() {return _jobsDao;}

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
}

package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.killers.JobKiller;
import edu.utexas.tacc.tapis.jobs.killers.JobKillerFactory;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobStatusMsg;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobExecutionUtils 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobExecutionUtils.class);
    
    // Job wrapper script name.
    public static final String JOB_WRAPPER_SCRIPT = "tapisjob.sh";
    public static final String JOB_ENV_FILE       = "tapisjob.env";
    
    // Docker command templates.
    private static final String DOCKER_ID = "docker ps -a --no-trunc -f \"%s\" --format \"{{.ID}}\"";
    private static final String DOCKER_STATUS = "docker ps -a --no-trunc -f \"name=%s\" --format \"{{.Status}}\"";
    private static final String DOCKER_RM = "docker rm %s";

    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    
    /* ********************************************************************** */
    /*                            Package Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* executeCmdMsg:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Process status or pause commands.
     * 
     * @param jobCtx the job's context
     * @param cmdMsg the asynchronous command received by the job
     * @param newStatus the new job status
     * @throws JobAsyncCmdException on success to stop further job processing
     */
    static void executeCmdMsg(JobExecutionContext jobCtx, CmdMsg cmdMsg, JobStatusType newStatus)
     throws JobAsyncCmdException
    {
        // Informational logging.
        Job job = jobCtx.getJob();
        String msg = MsgUtils.getMsg("JOBS_CMD_MSG_RECEIVED", job.getUuid(), cmdMsg.msgType.name(),
                                     cmdMsg.senderId, cmdMsg.correlationId);
        _log.info(msg);
        
        // Change the job state.  Failure here forces us to ignore the command.
        try {jobCtx.getJobsDao().setStatus(job, newStatus, msg);}
        catch (Exception e) {
            String msg1 = MsgUtils.getMsg("JOBS_STATUS_CHANGE_ON_CMD_ERROR", job.getUuid(), 
                                          newStatus.name(), cmdMsg.msgType.name());
            _log.error(msg1, e);
            return;  // the command failed 
        }
        
        // Best effort to kill the job on cancel.
        if (newStatus.isTerminal())
            try {
                // We never know if the attack worked.
                JobKiller killer = JobKillerFactory.getInstance(jobCtx);
                killer.attack();
            } catch (Exception e) {
                _log.warn(MsgUtils.getMsg("JOBS_CMD_KILL_ERROR", job.getUuid(), e.getMessage()));
            }

        // Stop further job processing on success.
        throw new JobAsyncCmdException(msg);
    }
    
    /* ---------------------------------------------------------------------- */
    /* executeCmdMsg:                                                         */
    /* ---------------------------------------------------------------------- */
    static void executeCmdMsg(JobExecutionContext jobCtx, JobStatusMsg cmdMsg)
    {
        // Informational logging.
        Job job = jobCtx.getJob();
        if (_log.isInfoEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_CMD_MSG_RECEIVED", job.getUuid(), cmdMsg.msgType.name(),
                                         cmdMsg.senderId, cmdMsg.correlationId);
            _log.info(msg);
        }
        
        // TODO: put job status on event queue
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDockerCidCommand:                                                   */
    /* ---------------------------------------------------------------------- */
    public static String getDockerCidCommand(String containerName)
    {return String.format(DOCKER_ID, containerName);}
    
    /* ---------------------------------------------------------------------- */
    /* getDockerStatusCommand:                                                */
    /* ---------------------------------------------------------------------- */
    public static String getDockerStatusCommand(String containerName)
    {return String.format(DOCKER_STATUS, containerName);}
    
    /* ---------------------------------------------------------------------- */
    /* getDockerRmCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    public static String getDockerRmCommand(String containerName)
    {return String.format(DOCKER_RM, containerName);}
    

}

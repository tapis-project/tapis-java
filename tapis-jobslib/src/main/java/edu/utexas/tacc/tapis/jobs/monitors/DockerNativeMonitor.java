package edu.utexas.tacc.tapis.jobs.monitors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public class DockerNativeMonitor 
 extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeMonitor.class);
    
    // Zero is recognized as the application success code.
    private static final String SUCCESS_RC = "0";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The application return code as reported by docker.
    private String _exitCode;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DockerNativeMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {super(jobCtx, policy);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getRC:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Return the application exit code as reported by docker.  If no exit
     * code has been ascertained, null is returned. 
     * 
     * @return the application exit code or null
     */
    @Override
    public String getExitCode() {return _exitCode;}
    
    /* ---------------------------------------------------------------------- */
    /* monitorQueuedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorQueuedJob() throws TapisException
    {
        // The queued state is a no-op for forked jobs.
    }

    /* ---------------------------------------------------------------------- */
    /* queryRemoteJob:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Query the status of the job's container.  When the container terminates,
     * this method captures the application's exit code and makes it available
     * to the caller.
     * 
     */
    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException
    {
        // There's no difference between the active and inactive docker queries, 
        // so there's no point in issuing a second (inactive) query if the first
        // one did not return a result.
        if (!active) return JobRemoteStatus.NULL;
        
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Get the command object.
        var runCmd = new TapisRunCommand(_jobCtx.getExecutionSystem(), conn);
        
        // Get the command text for this job's container.
        String cmd = JobExecutionUtils.getDockerStatusCommand(_job.getUuid());
        
        // Query the container.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                return JobRemoteStatus.NULL;
            }
        
        // We should have gotten something.
        if (StringUtils.isBlank(result)) return JobRemoteStatus.EMPTY;
        
        // Is the container in a non-terminal state?
        if (result.startsWith(JobExecutionUtils.DOCKER_ACTIVE_STATUS_PREFIX))
            return JobRemoteStatus.ACTIVE;
        
        // Has the container terminated?  If so, we always set the application
        // exit code and return the status from within this block.  We also
        // remote the container from the execution system.
        if (result.startsWith(JobExecutionUtils.DOCKER_INACTIVE_STATUS_PREFIX)) 
        {
            // The status that will be returned.
            JobRemoteStatus status;
            
            // We expect a status string that looks like "Exited (0) 41 seconds ago".
            // Group 1 in the regex match isolates the parenthesized return code.
            var m = JobExecutionUtils.DOCKER_RC_PATTERN.matcher(result);
            if (m.matches()) {
                _exitCode = m.group(1);
                if (SUCCESS_RC.equals(_exitCode)) status = JobRemoteStatus.DONE;
                  else status = JobRemoteStatus.FAILED;
            } else {
                String msg = MsgUtils.getMsg("JOBS_DOCKER_STATUS_PARSE_ERROR", 
                                             _job.getUuid(), result, cmd);
                _log.warn(msg);
                _exitCode = SUCCESS_RC;
                status = JobRemoteStatus.DONE;
            }
            
            // Remove the container from the execution system.
//            removeContainer(_jobCtx.getExecutionSystem(), conn);
            
            return status;
        }
        
        // This should not happen.
        String msg = MsgUtils.getMsg("JOBS_DOCKER_STATUS_PARSE_ERROR", 
                                     _job.getUuid(), result, cmd);
        _log.error(msg);
        return JobRemoteStatus.EMPTY;
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TSystem execSystem, SSHConnection conn)
    {
        // Get the command object.
        var runCmd = new TapisRunCommand(execSystem, conn);
        
        // Get the command text for this job's container.
        String cmd = JobExecutionUtils.getDockerRmCommand(_job.getUuid());
        
        // Query the container.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                String cid = _job.getRemoteJobId();
                if (!StringUtils.isBlank(cid) && cid.length() >= 12) cid = cid.substring(0, 12);
                String msg = MsgUtils.getMsg("JOBS_DOCKER_RM_CONTAINER_ERROR", 
                                             _job.getUuid(), execSystem.getId(), cid, result, cmd);
                _log.error(msg, e);
            }
    }
}

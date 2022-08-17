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
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public class DockerNativeMonitor 
 extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeMonitor.class);
    
    // Distinguished error strings.
    private static final String ERROR_PERMISSION_DENIED = "permission denied";
    
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
    /* getExitCode:                                                           */
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
        
        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        
        // Get the command text for this job's container.
        String cmd = JobExecutionUtils.getDockerStatusCommand(_job.getUuid());
        
        // Query the container.
        String result = null;
        int rc;
        try {
            // Issue the command and get the result.
            rc = runCmd.execute(cmd);
            runCmd.logNonZeroExitCode();
            result = runCmd.getOutAsString();
        }
        catch (Exception e) {
            _log.error(e.getMessage(), e);
            return JobRemoteStatus.NULL;
        }
        
        // Determine if there's no point in going on.
        detectFatalCondition(rc, result);
        
        // We should have gotten something.
        if (StringUtils.isBlank(result)) return JobRemoteStatus.EMPTY;
        
        // Is the container in a non-terminal state?
        if (result.startsWith(JobExecutionUtils.DOCKER_ACTIVE_STATUS_PREFIX))
            return JobRemoteStatus.ACTIVE;
        
        // Has the container terminated?  If so, we always set the application
        // exit code and return the status from within this block.  We also
        // remove the container from the execution system.
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
            removeContainer(_jobCtx.getExecutionSystem(), runCmd);
            
            return status;
        }
        
        // This should not happen.
        String msg = MsgUtils.getMsg("JOBS_DOCKER_STATUS_PARSE_ERROR", 
                                     _job.getUuid(), result, cmd);
        _log.error(msg);
        return JobRemoteStatus.EMPTY;
    }
    
    /* ---------------------------------------------------------------------- */
    /* detectFatalCondition:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Fail fast under certain conditions, otherwise let the monitoring policy 
     * play out.  
     * 
     * We take a conservative approach to ending the job due to a monitoring
     * call error because we don't want transient errors to kill a job.  Under
     * this approach, we'll add particular conditions that we know are 
     * unrecoverable as we encounter them.  At some point we might decide that 
     * any error whose result string contains "docker: " are always 
     * unrecoverable.
     * 
     * @param rc the return code from the remote command
     * @param result error message, possibly null
     * @throws TapisException 
     */
    private void detectFatalCondition(int rc, String result) 
     throws TapisException
    {
        // No error or we don't have any information on which to
        // choose whether or not to handle the error condition.
        if (rc == 0 || result == null) return;
        
        // Unable to talk to docker daemon, probably due to the authenticated user
        // on the execution system not able to write the daemon's unix socket.
        if (result.contains(ERROR_PERMISSION_DENIED)) {
            String host = "<unknown>";
            try {host = _jobCtx.getExecutionSystem().getHost();} catch (Exception e) {}
            String msg = MsgUtils.getMsg("JOBS_DOCKER_PERM_DENIED", rc, 
                                         _job.getOwner(), _job.getUuid(), host);
            throw new TapisException(msg);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TapisSystem execSystem, TapisRunCommand runCmd)
    {
        // Get the command text for this job's container.
        String cmd = JobExecutionUtils.getDockerRmCommand(_job.getUuid());
        
        // Query the container.
        String result = null;
        try {
            int exitCode = runCmd.execute(cmd);
            _log.debug("Monitor: removeContainer exitCode = " + exitCode);
            if (exitCode != 0 && _log.isWarnEnabled()) 
                _log.warn(MsgUtils.getMsg("TAPIS_SSH_CMD_ERROR", cmd, 
                                          runCmd.getConnection().getHost(), 
                                          runCmd.getConnection().getUsername(), 
                                          exitCode));
            result = runCmd.getOutAsString();
        }
        catch (Exception e) {
            String cid = _job.getRemoteJobId();
            if (!StringUtils.isBlank(cid) && cid.length() >= 12) cid = cid.substring(0, 12);
            String msg = MsgUtils.getMsg("JOBS_DOCKER_RM_CONTAINER_ERROR", 
                                         _job.getUuid(), execSystem.getId(), cid, result, cmd);
            _log.error(msg, e);
        }
    }
}

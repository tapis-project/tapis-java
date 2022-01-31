package edu.utexas.tacc.tapis.jobs.launchers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class DockerNativeLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeLauncher.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DockerNativeLauncher(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Create and populate the docker command.
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* launch:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public void launch() throws TapisException
    {
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Start the container.
        var runCmd     = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        runCmd.logNonZeroExitCode();
        String result  = runCmd.getOutAsString();
        if (StringUtils.isBlank(result)) result = "";
        
        // Let's see what happened.
        String cid = UNKNOWN_CONTAINER_ID;
        if (exitStatus == 0) {
            cid = result.trim();
            if (StringUtils.isBlank(cid)) cid = UNKNOWN_CONTAINER_ID;
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                             _job.getUuid(), cid, exitStatus);
                _log.debug(msg);
            }
        } else {
            // Our one chance at launching the container failed with a non-communication
            // error, which we assume is unrecoverable so we abort the job now.
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result, exitStatus);
            throw new TapisException(msg);
        }

        // Save the container id or the unknown id string.
        _jobCtx.getJobsDao().setRemoteJobId(_job, cid);
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getRemoteIdCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Get the remote command that would return the docker container id.
     * Not currently called. 
     * @return the command that returns the container id
     */
    private String getRemoteIdCommand()
    {
        return JobExecutionUtils.getDockerCidCommand(_job.getUuid());
    }
}

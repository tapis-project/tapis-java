package edu.utexas.tacc.tapis.jobs.launchers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Launch a job using singularity run from a wrapper script that returns the
 * PID of the spawned background process.
 * 
 * @author rcardone
 */
public final class SingularityRunLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunLauncher.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunLauncher(JobExecutionContext jobCtx)
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
        // Throttling adds a randomized delay on heavily used hosts.
        throttleLaunch();
        
        // -------------------- Launch Container --------------------
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        
        // Start the container and retrieve the pid.
        int exitCode  = runCmd.execute(cmd);
        String result = runCmd.getOutAsString();
        
        // Let's see what happened.
        if (exitCode != 0) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result, exitCode);
            throw new JobException(msg);
        }

        // Note success.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                         _job.getUuid(), result, exitCode);
            _log.debug(msg);
        }
        
        // -------------------- Get PID -----------------------------
        // We have a problem if the result is not the pid.
        if (StringUtils.isBlank(result)) {
            String msg = MsgUtils.getMsg("JOBS_SINGULARITY_RUN_NO_PID_ERROR", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd);
            throw new JobException(msg);
        }
        
        // Make sure the pid is an integer.
        String pid = result.trim();
        try {Integer.valueOf(pid);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_RUN_INVALID_PID_ERROR", getClass().getSimpleName(), 
                                             _job.getUuid(), cmd, pid);
                throw new JobException(msg, e);
            }
        
        // Save the container id.
        _jobCtx.getJobsDao().setRemoteJobId(_job, pid);
    }
}

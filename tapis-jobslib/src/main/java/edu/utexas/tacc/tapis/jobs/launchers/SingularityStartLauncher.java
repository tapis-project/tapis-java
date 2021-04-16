package edu.utexas.tacc.tapis.jobs.launchers;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

public final class SingularityStartLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityStartLauncher.class);
    
    // Remote empty result.
    private static final String NO_RESULT = "<no result>";
    
    // Split text on whitespace.
    private static final Pattern wsPattern = Pattern.compile("\\s");

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityStartLauncher(JobExecutionContext jobCtx)
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
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Get the command object.
        var runCmd = new TapisRunCommand(_jobCtx.getExecutionSystem(), conn);
        
        // -------------------- Launch Container --------------------
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Start the container.
        String result  = runCmd.execute(cmd);
        int exitStatus = runCmd.getExitStatus();
        
        // Let's see what happened.
        if (!StringUtils.isBlank(result)) result = result.trim();
          else result = NO_RESULT;
        if (exitStatus == 0) {
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                             _job.getUuid(), result, exitStatus);
                _log.debug(msg);
            }
        } else {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_WARN", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result, exitStatus);
            _log.warn(msg);
        }

        // -------------------- Get PID -----------------------------
        // Get the singularity list command for this container.
        String idCmd = getRemoteIdCommand();
        
        // List the container.
        result = runCmd.execute(idCmd);
        exitStatus = runCmd.getExitStatus();
        String pid = extractPid(result);
        
        // We cannot monitor without a pid.
        if (pid == null) {
            // **** error
        }
        
        // Save the container id or the unknown id string.
        _jobCtx.getJobsDao().setRemoteJobId(_job, pid);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRemoteIdCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Get the remote command that singularity pid.
     * Not currently called. 
     * @return the command that returns the container id
     */
    @Override
    protected String getRemoteIdCommand()
    {
        return JobExecutionUtils.SINGULARITY_START_PID + _job.getUuid();
    }
    
    /* ---------------------------------------------------------------------- */
    /* extractPid:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Extract the pid from the result that looks like this:
     * 
     *  INSTANCE NAME                               PID        IP    IMAGE
     *  07872ed9-b816-4888-90c4-9dbaa35b601e-007    4060848          myapp.sif
     * 
     * where the job uuid is the name of the instance.  If the instance isn't
     * found, null is returned. 
     * 
     * @param result the singularity list result
     * @return the pid of the container we just started 
     */
    private String extractPid(String result)
    {
        // This is not good.
        if (StringUtils.isBlank(result)) return null;
        
        // The pid immediately follows the instance name,
        // which is the job uuid.
        var parts = wsPattern.split(result);
        for (int i = 0; i < parts.length; i++)
            if (parts[i].equals(_job.getUuid()) && (i+1 < parts.length))
                return parts[i+1];
                
        // The instance was not found.
        return null;
    }
}

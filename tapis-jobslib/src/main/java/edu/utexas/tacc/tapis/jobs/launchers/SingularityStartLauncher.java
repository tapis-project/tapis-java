package edu.utexas.tacc.tapis.jobs.launchers;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

/** Launch a job using singularity instance start and singularity instance list
 * to retrieve the root PID of the spawned process.
 * 
 * @author rcardone
 */
public final class SingularityStartLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityStartLauncher.class);
    
    // Remote empty result.
    private static final String NO_RESULT = "<no result text>";
    
    // Split text on whitespace.
    private static final Pattern _wsPattern = Pattern.compile("\\s+");

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
        // -------------------- Launch Container --------------------
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Get the command object.
        var runCmd = new TapisRunCommand(_jobCtx.getExecutionSystem(), conn);
        
        // -------------------- Rollback Area -----------------------
        boolean killContainer = false; // set when exceptions are thrown
        try {
            // Start the container.
            String result  = runCmd.execute(cmd);
            int exitStatus = runCmd.getExitStatus();
        
            // Let's see what happened.
            if (!StringUtils.isBlank(result)) result = result.trim();
              else result = NO_RESULT;
            if (exitStatus != 0) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(), 
                                             _job.getUuid(), cmd, result, exitStatus);
                throw new JobException(msg);
            }
            
            // Note success.  
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                             _job.getUuid(), result, exitStatus);
                _log.debug(msg);    
            }

            // -------------------- Get PID -----------------------------
            // Run the singularity list command for this container.
            String idCmd = getRemoteIdCommand();
            result = runCmd.execute(idCmd);
            exitStatus = runCmd.getExitStatus();
            if (exitStatus != 0) {
                // Issue a warning here, we'll determine if the problem is fatal below.
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_LIST_PID_WARN", getClass().getSimpleName(), 
                                             _job.getUuid(), idCmd, exitStatus);
                _log.warn(msg);
            }
        
            // We cannot monitor without a pid.
            String pid = extractPid(result);
            if (StringUtils.isBlank(pid)) {
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_LIST_PID_ERROR", getClass().getSimpleName(), 
                                             _job.getUuid(), idCmd, result);
                throw new JobException(msg);    
            }
        
            // Save the container id.
            _jobCtx.getJobsDao().setRemoteJobId(_job, pid);
        }
        // Try to kill the container on any exception in the rollback area.
        catch (Exception e) {killContainer = true; throw e;}
        finally {
            // If we hit an exception in the rollback area, we kill the job.
            if (killContainer) removeContainer(runCmd);
        }
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getRemoteIdCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Get the remote command that singularity pid.
     * Not currently called. 
     * @return the command that returns the container id
     */
    private String getRemoteIdCommand()
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
     * @return the pid of the container we just started or null
     */
    private String extractPid(String result)
    {
        // This is not good.
        if (StringUtils.isBlank(result)) return null;
        
        // The pid immediately follows the instance name,
        // which is the job uuid.
        var parts = _wsPattern.split(result);
        for (int i = 0; i < parts.length; i++)
            if (parts[i].equals(_job.getUuid()) && (i+1 < parts.length))
                return parts[i+1];
                
        // The instance was not found.
        return null;
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TapisRunCommand runCmd) 
    {
        // Get the command text to terminate this job's singularity instance.
        String cmd = JobExecutionUtils.SINGULARITY_STOP + _job.getUuid();
        
        // Stop the instance.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                String execSysId = null;
                try {execSysId = _jobCtx.getExecutionSystem().getId();} catch (Exception e1) {}
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_RM_CONTAINER_ERROR", 
                                             _job.getUuid(), execSysId, result, cmd);
                _log.error(msg, e);
            }
    }
}

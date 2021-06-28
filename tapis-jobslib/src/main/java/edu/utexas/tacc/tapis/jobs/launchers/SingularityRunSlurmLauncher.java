package edu.utexas.tacc.tapis.jobs.launchers;

import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Launch a job using singularity run from a wrapper script that returns the
 * PID of the spawned background process.
 * 
 * @author rcardone
 */
public final class SingularityRunSlurmLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmLauncher.class);

    // Initialize the regex pattern that extracts the ID slurm assigned to the job.
    // The regex ignores leading and trailing whitespace and groups the numeric ID.
    private static final Pattern _resultPattern = Pattern.compile("\\s*Submitted batch job (\\d+)\\s*");
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmLauncher(JobExecutionContext jobCtx)
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
        
        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        
        // Start the container and retrieve the pid.
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsString();
        
        // Let's see what happened.
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
        
        // -------------------- Get ID ------------------------------
        // Extract the slurm id.
        String id = getSlurmId(result, cmd);
        
        // Save the id.
        _jobCtx.getJobsDao().setRemoteJobId(_job, id);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getLaunchCommand:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getLaunchCommand() 
     throws TapisException
    {
        // Create the command that changes the directory to the execution 
        // directory and submits the job script.  The directory is expressed
        // as an absolute path on the system.
        String cmd = "cd " + Paths.get(_jobCtx.getExecutionSystem().getRootDir(), 
                                       _job.getExecSystemExecDir()).toString();
        cmd += ";sbatch " + JobExecutionUtils.JOB_WRAPPER_SCRIPT;
        return cmd;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getSlurmId:                                                            */
    /* ---------------------------------------------------------------------- */
    private String getSlurmId(String output, String cmd) throws JobException
    {
        // We have a problem if the result is not the slurm id.
        if (StringUtils.isBlank(output)) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_NO_RESULT",  
                                         _job.getUuid(), cmd);
            throw new JobException(msg);
        }
        
        // Look for the success message
        Matcher m = _resultPattern.matcher(output);
        var found = m.matches();
        if (!found) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_INVALID_RESULT",  
                                         _job.getUuid(), output);
            throw new JobException(msg);
        }
        
        int groupCount = m.groupCount();
        if (groupCount < 1) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_INVALID_RESULT",  
                                         _job.getUuid(), output);
            throw new JobException(msg);
        }
        
        // Group 1 contains the slurm ID.
        return m.group(1);
    }
}

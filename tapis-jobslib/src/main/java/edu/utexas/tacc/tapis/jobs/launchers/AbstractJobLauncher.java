package edu.utexas.tacc.tapis.jobs.launchers;

import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

abstract class AbstractJobLauncher
 implements JobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobLauncher.class);

    // Special transfer id value indicating no files to stage.
    private static final String UNKNOWN_CONTAINER_ID = "<Unknown-Container-ID>";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractJobLauncher(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
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
        String cid = UNKNOWN_CONTAINER_ID;       
        if (exitStatus == 0 && !StringUtils.isBlank(result)) {
            cid = result.trim();
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                             _job.getUuid(), cid, exitStatus);
                _log.debug(msg);
            }
        } else {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_WARN", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result, exitStatus);
            _log.warn(msg);
        }

        // Save the container id or the unknown id string.
        if (StringUtils.isBlank(cid)) cid = UNKNOWN_CONTAINER_ID;
        _jobCtx.getJobsDao().setRemoteJobId(_job, cid);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getLaunchCommand:                                                      */
    /* ---------------------------------------------------------------------- */
    protected String getLaunchCommand() 
     throws TapisServiceConnectionException, TapisImplException
    {
        // Create the command that changes the directory to the execution 
        // directory and runs the wrapper script.  The directory is expressed
        // as an absolute path on the system.
        String cmd = "cd " + Paths.get(_jobCtx.getExecutionSystem().getRootDir(), 
                                       _job.getExecSystemExecDir()).toString();
        cmd += ";./" + JobExecutionUtils.JOB_WRAPPER_SCRIPT;
        return cmd;
    }

    /* ---------------------------------------------------------------------- */
    /* getRemoteIdCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    protected abstract String getRemoteIdCommand();
}    

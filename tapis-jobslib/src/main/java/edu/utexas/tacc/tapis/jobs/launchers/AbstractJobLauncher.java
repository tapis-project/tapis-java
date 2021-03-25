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
        if (_log.isInfoEnabled()) 
            _log.info(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                      _job.getUuid(), cmd));
        
        // Start the container.
        String result = runCmd.execute(cmd);

        // We expect there to be no result.
        if (!StringUtils.isBlank(result)) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_WARN", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result);
            _log.warn(msg);
        }
                
        // Get and save the container id. The container name is always the job uuid.  
        // Account for slow docker execution by retrying here.  We'll wait for 15 
        // seconds at most.
        result = null;
        final int iterations   = 4;
        final int sleepMillis  = 5000;
        String cmd2 = getRemoteIdCommand(); // subclass call.
        for (int i = 0; i < iterations; i++) {
            // Sleep on all iterations other than the first.
            if (i != 0) try {Thread.sleep(sleepMillis);} catch (Exception e) {}
            
            // Query for the container id.
            try {result = runCmd.execute(cmd2);}
                catch (Exception e) {
                    int attemptsLeft = (iterations - 1) - i;
                    String msg = MsgUtils.getMsg("JOBS_GET_CID_ERROR", getClass().getSimpleName(), 
                                                 _job.getUuid(), cmd2, attemptsLeft, e.getMessage());
                    _log.error(msg, e);
                    continue;
                } 
            
            // We expect the full container id to be returned.
            if (StringUtils.isBlank(result)) {
                int attemptsLeft = (iterations - 1) - i;
                String msg = MsgUtils.getMsg("JOBS_GET_CID_ERROR", getClass().getSimpleName(), 
                                             _job.getUuid(), cmd2, attemptsLeft, "empty result");
                _log.error(msg);
                continue;
            }
            
            // We got an id.
            break;
        }
        
        // Save the container id or the unknown id string.
        if (StringUtils.isBlank(result)) result = UNKNOWN_CONTAINER_ID;
        _jobCtx.getJobsDao().setRemoteJobId(_job, result);
    }
    
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

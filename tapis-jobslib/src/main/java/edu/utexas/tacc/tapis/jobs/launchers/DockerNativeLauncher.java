package edu.utexas.tacc.tapis.jobs.launchers;

import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

public final class DockerNativeLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeLauncher.class);

    // Special transfer id value indicating no files to stage.
    private static final String UNKNOWN_CONTAINER_ID = "<Unknown-Container-ID>";
    
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
        
        // Create the command that changes the directory to the execution 
        // directory and runs the wrapper script.  The directory is expressed
        // as an absolute path on the system.
        String cmd = "cd " + Paths.get(_jobCtx.getExecutionSystem().getRootDir(), 
                                        _job.getExecSystemExecDir()).toString();
        cmd += ";./" + JobExecutionUtils.JOB_WRAPPER_SCRIPT;
        
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
        for (int i = 0; i < iterations; i++) {
            // Sleep on all iterations other than the first.
            if (i != 0) try {Thread.sleep(sleepMillis);} catch (Exception e) {}
            
            // Query for the container id.
            try {result = runCmd.execute(JobExecutionUtils.getDockerCidCommand(_job.getUuid()));}
                catch (Exception e) {
                    int attemptsLeft = (iterations - 1) - i;
                    String msg = MsgUtils.getMsg("JOBS_GET_CID_ERROR", getClass().getSimpleName(), 
                                                 _job.getUuid(), cmd, attemptsLeft, e.getMessage());
                    _log.error(msg, e);
                    continue;
                } 
            
            // We expect the full container id to be returned.
            if (StringUtils.isBlank(result)) {
                int attemptsLeft = (iterations - 1) - i;
                String msg = MsgUtils.getMsg("JOBS_GET_CID_ERROR", getClass().getSimpleName(), 
                                             _job.getUuid(), cmd, attemptsLeft, "empty result");
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
}

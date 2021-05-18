package edu.utexas.tacc.tapis.jobs.launchers;

import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;

abstract class AbstractJobLauncher
 implements JobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobLauncher.class);

    // Special transfer id value indicating no files to stage.
    protected static final String UNKNOWN_CONTAINER_ID = "<Unknown-Container-ID>";
    protected static final String UNKNOWN_PROCESS_ID   = "<Unknown-Process-ID>";
    
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
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getLaunchCommand:                                                      */
    /* ---------------------------------------------------------------------- */
    protected String getLaunchCommand() 
     throws TapisException
    {
        // Create the command that changes the directory to the execution 
        // directory and runs the wrapper script.  The directory is expressed
        // as an absolute path on the system.
        String cmd = "cd " + Paths.get(_jobCtx.getExecutionSystem().getRootDir(), 
                                       _job.getExecSystemExecDir()).toString();
        cmd += ";./" + JobExecutionUtils.JOB_WRAPPER_SCRIPT;
        return cmd;
    }
}    

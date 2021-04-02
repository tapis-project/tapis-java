package edu.utexas.tacc.tapis.jobs.stagers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public abstract class AbstractJobExecStager 
 implements JobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobExecStager.class);
    
    // Command buffer initial capacity.
    private static final int INIT_CMD_LEN = 2048;

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Input parameters
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;
    
    // The buffer used to build command file content. 
    protected final StringBuilder       _cmd;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractJobExecStager(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
        
        // Initialize the command file text.
        _cmd = new StringBuilder(INIT_CMD_LEN);
    }


    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public void stageJob() throws TapisException 
    {
        // Create the wrapper script.
        String wrapperScript = generateWrapperScript();
        
        // Create the environment variable definition file.
        String envVarFile = generateEnvVarFile();
        
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var fm = new JobFileManager(_jobCtx);
        
        // Install the wrapper script on the execution system.
        fm.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);
        
        // Install the env variable definition file.
        fm.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content.
     * 
     * @return the wrapper script content
     */
    protected abstract String generateWrapperScript() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for a environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    protected abstract String generateEnvVarFile() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* initBashScript:                                                        */
    /* ---------------------------------------------------------------------- */
    protected void initBashScript()
    {
        _cmd.append("#!/bin/bash\n\n");
        appendDescription();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* appendDescription:                                                     */
    /* ---------------------------------------------------------------------- */
    private void appendDescription()
    {
        _cmd.append("# This script was auto-generated by the Tapis Jobs Service for the purpose\n");
        _cmd.append("# of running a Tapis application.  The order of execution is as follows:\n");
        _cmd.append("#\n");
        _cmd.append("#   1. Standard Tapis and user-supplied environment variables are exported.\n");
        _cmd.append("#   2. The application container is run with container options, environment\n");
        _cmd.append("#      variables and application parameters as specified in the Tapis job,\n");
        _cmd.append("#       application and system definitions.\n");
        _cmd.append("\n");
    }
}

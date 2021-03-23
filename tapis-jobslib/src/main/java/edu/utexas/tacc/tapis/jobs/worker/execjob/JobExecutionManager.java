package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.launchers.JobLauncher;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stager.JobExecStageFactory;
import edu.utexas.tacc.tapis.jobs.stager.JobExecStager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public final class JobExecutionManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobExecutionManager.class);
    
    // Special transfer id value indicating no files to stage.
    private static final String NO_FILE_INPUTS = "no inputs";

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final JobExecutionContext _jobCtx;
    private final Job                 _job;
    
    // The managers for different execution phases.
    private JobExecStager             _jobStager;
    private JobLauncher               _jobLauncher;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobExecutionManager(JobExecutionContext ctx)
    {
        _jobCtx = ctx;
        _job = ctx.getJob();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    public void stageJob() throws TapisException
    {
        // Assign the stager for this job.
        _jobStager = JobExecStageFactory.getInstance(_jobCtx);
        
        // Create the wrapper script.
        String wrapperScript = _jobStager.generateWrapperScript();
        
        // Create the environment variable definition file.
        String envVarFile = _jobStager.generateEnvVarFile();
        
        // Install the wrapper script on the execution system.
        installWrapperScript(wrapperScript);
        
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* installWrapperScript:                                                  */
    /* ---------------------------------------------------------------------- */
    private void installWrapperScript(String wrapperScript)
    {
        
    }
}

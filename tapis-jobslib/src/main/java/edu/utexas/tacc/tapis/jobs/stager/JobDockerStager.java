package edu.utexas.tacc.tapis.jobs.stager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.stager.runtimes.DockerRunCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;

public final class JobDockerStager 
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobDockerStager.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobDockerStager(JobExecutionContext jobCtx){super(jobCtx);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateWrapperScript() 
    {
        // Create and populate the docker command.
        var dockerCmd = new DockerRunCmd();
        
        
        return null;
    }
}

package edu.utexas.tacc.tapis.jobs.launchers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.launchers.parsers.JobLaunchResult;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public final class DockerNativeLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeLauncher.class);

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
    public JobLaunchResult launch() 
    {
        // TODO Auto-generated method stub
        return null;
    }
}

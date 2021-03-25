package edu.utexas.tacc.tapis.jobs.launchers;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public final class DockerNativeLauncher 
 extends AbstractJobLauncher
{
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
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getRemoteIdCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getRemoteIdCommand()
    {
        return JobExecutionUtils.getDockerCidCommand(_job.getUuid());
    }
}

package edu.utexas.tacc.tapis.jobs.stager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;

public abstract class AbstractJobExecStager 
 implements JobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobExecStager.class);

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
    protected AbstractJobExecStager(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
    }

}

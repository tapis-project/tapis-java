package edu.utexas.tacc.tapis.jobs.monitors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

abstract class AbstractJobMonitor
 implements JobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobMonitor.class);

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
    protected AbstractJobMonitor(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* monitorQueuedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorQueuedJob() throws TapisException
    {
        
    }

    /* ---------------------------------------------------------------------- */
    /* monitorRunningJob:                                                     */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorRunningJob() throws TapisException
    {
        
    }

    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* allowEmptyResult:                                                      */
    /* ---------------------------------------------------------------------- */
    /** This method determines whether the result of a monitoring 
     * request can be empty or not.  When true is returned, empty
     * results from a monitor query do not cause an exception to
     * be thrown.  When false is returned, the remote client code
     * considers an empty response to be an error and throws an
     * exception.
     * 
     * @return true to allow empty monitoring results, false otherwise
     */
    protected boolean allowEmptyResult() {return false;}
}

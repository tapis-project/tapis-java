package edu.utexas.tacc.tapis.jobs.cancellers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;

public abstract class AbstractJobCanceler implements JobCanceler{
	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
	private static final Logger _log = LoggerFactory.getLogger(AbstractJobCanceler.class);
	
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
    protected AbstractJobCanceler(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
    }
    
   
}

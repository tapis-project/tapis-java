package edu.utexas.tacc.tapis.jobs.monitors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class DockerNativeMonitor 
 extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeMonitor.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DockerNativeMonitor(JobExecutionContext jobCtx)
    {super(jobCtx);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* monitor:                                                               */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitor() throws TapisException {
        
    }

    /* ---------------------------------------------------------------------- */
    /* allowEmptyResult:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    public boolean allowEmptyResult() {return false;}

}

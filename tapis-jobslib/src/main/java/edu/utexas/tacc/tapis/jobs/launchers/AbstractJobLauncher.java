package edu.utexas.tacc.tapis.jobs.launchers;

import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.utils.ThrottleMap;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

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
    
    // Thread group name used in ThrottleMap, throttle parameters and launch 
    // delay parameters.  The duration of the throttle window and the limit on
    // the number of launches within that window are chosen to avoid overwhelming
    // a host with ssh connections and commands.  The delay is specified as a 
    // minimum and a maximum skew.  The actual delay is the minimum plus a 
    // randomized number of milliseconds up to the skew.
    private static final String THROTTLEMAP_NAME = "LauncherThrottleMap";
    private static final int    THROTTLE_SECONDS = 2;
    private static final int    THROTTLE_LIMIT   = 8;
    private static final int    JOB_LAUNCH_DELAY_MS    = 3000;
    private static final int    JOB_LAUNCH_MAX_SKEW_MS = 60000;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;
    
    // Map of host name to throttle entries used to control the number of launch 
    // issued to a host within a time window.
    private static final ThrottleMap    _hostThrottles = initHostThrottles();

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
    
    /* ---------------------------------------------------------------------- */
    /* throttleLaunch:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Delay launches when too many have recently taken place on a host.
     */
    protected void throttleLaunch()
    {
        // Get the host on which the job will be launched.
        String host = null;
        try {
            // Return from here if there's room in the throttle's sliding
            // window for this host.
            host = _jobCtx.getExecutionSystem().getHost();
            if (_hostThrottles.record(host)) return;
        } catch (Exception e) {
            // Abort throttling if we hit an error.
            String msg = MsgUtils.getMsg("JOB_HOST_RETRIEVAL_ERROR", _job.getUuid());
            _log.error(msg);
            return;
        }
        
        // This host needs to be throttled.
        // Calculate a randomized but short delay in milliseconds.
        var skewMs = ThreadLocalRandom.current().nextInt(JOB_LAUNCH_MAX_SKEW_MS);
        skewMs += JOB_LAUNCH_DELAY_MS;
        
        // Log the delay.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_DELAYED_LAUNCH", _job.getUuid(), skewMs, host));
        
        // Delay for the randomized period.
        try {Thread.sleep(skewMs);} catch (InterruptedException e) {}
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initHostThrottles:                                                     */
    /* ---------------------------------------------------------------------- */
    private static ThrottleMap initHostThrottles()
    {
        return new ThrottleMap(THROTTLEMAP_NAME, THROTTLE_SECONDS, THROTTLE_LIMIT);
    }
}    

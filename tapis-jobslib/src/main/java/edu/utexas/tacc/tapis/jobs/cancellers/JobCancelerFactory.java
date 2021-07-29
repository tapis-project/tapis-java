package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppTypeEnum;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.launchers.JobLauncher;
import edu.utexas.tacc.tapis.jobs.monitors.DockerNativeMonitor;
import edu.utexas.tacc.tapis.jobs.monitors.JobMonitor;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class JobCancelerFactory {

	/* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a launcher based on the type of job and its execution environment.
     * This method either returns the appropriate luuncher or throws an exception.
     * 
     * @param jobCtx job context
     * @return the launcher designated for the current job type and environment
     * @throws TapisException when no launcher is found or a network error occurs
     */
    public static JobCanceler getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
    	 // Extract required information from app.
        var app = jobCtx.getApp();
        var appType = app.getAppType();
        var runtime = app.getRuntime();
        
        // The result.
        JobCanceler canceler = null;
        
        // ------------------------- FORK -------------------------
       if (appType == AppTypeEnum.FORK) {
    	   canceler = switch (runtime) {
                case DOCKER      -> new DockerNativeCanceler(jobCtx);
                //case SINGULARITY -> getSingularityOption(jobCtx, policy, app);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobCancelerFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
       /* else if (appType == AppTypeEnum.BATCH) {
            // Get the scheduler under which containers were launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Doublecheck that a scheduler is assigned.
            if (scheduler == null) {
                String msg = MsgUtils.getMsg("JOBS_SYSTEM_MISSING_SCHEDULER", system.getId(), 
                                              jobCtx.getJob().getUuid());
                throw new JobException(msg);
            }
            
            // Get the canceler for each supported runtime/scheduler combination.
            monitor = switch (runtime) {
                //case DOCKER      -> getBatchDockerCanceler(jobCtx, scheduler);
                //case SINGULARITY -> getBatchSingularityCanceler(jobCtx, policy, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobCancelerFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_TYPE", appType, "JobCancelerFactory");
            throw new JobException(msg);
        }*/
		return canceler;
    	
    }

}

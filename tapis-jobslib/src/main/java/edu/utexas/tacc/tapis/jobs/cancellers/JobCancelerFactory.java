package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppTypeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.launchers.JobLauncher;
import edu.utexas.tacc.tapis.jobs.launchers.SingularityRunLauncher;
import edu.utexas.tacc.tapis.jobs.launchers.SingularityStartLauncher;
import edu.utexas.tacc.tapis.jobs.monitors.DockerNativeMonitor;
import edu.utexas.tacc.tapis.jobs.monitors.JobMonitor;
import edu.utexas.tacc.tapis.jobs.monitors.SlurmMonitor;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

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
                case SINGULARITY -> getSingularityOption(jobCtx, app);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobCancelerFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (appType == AppTypeEnum.BATCH) {
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
            canceler = switch (runtime) {
                case DOCKER      -> getBatchDockerCanceler(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityCanceler(jobCtx, scheduler);
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
        }
		return canceler;
    	
    }
    /* ---------------------------------------------------------------------- */
    /* getSingularityOption:                                                  */
    /* ---------------------------------------------------------------------- */
    private static JobCanceler getSingularityOption(JobExecutionContext jobCtx,
                                                    TapisApp app)
     throws TapisException
    {
        // We are only interested in the singularity options.  These have
        // been validated in JobExecStageFactory, so no need to repeat here.
        var opts = app.getRuntimeOptions();
        boolean start = opts.contains(RuntimeOptionEnum.SINGULARITY_START);
        
        // Create the specified canceler.
        if (start) return new SingularityStartCanceler(jobCtx);
          else return new SingularityRunCanceler(jobCtx);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getBatchDockerMonitor:                                                 */
    /* ---------------------------------------------------------------------- */
    private static JobCanceler getBatchDockerCanceler(JobExecutionContext jobCtx,
                                                      SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker monitor. 
    	JobCanceler canceler = switch (scheduler) {
            case SLURM -> null; // not implemented
            
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler.name(), "JobCancelerFactory");
                throw new JobException(msg);
            }
        };
        
        return canceler;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityMonitor:                                            */
    /* ---------------------------------------------------------------------- */
    private static JobCanceler getBatchSingularityCanceler(JobExecutionContext jobCtx,
                                                           SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's slurm canceler. 
        JobCanceler canceler = switch (scheduler) {
            case SLURM -> new SlurmCanceler(jobCtx);
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler.name(), "JobMonitorFactory");
                throw new JobException(msg);
            }
        };
        
        return canceler;
    }

}

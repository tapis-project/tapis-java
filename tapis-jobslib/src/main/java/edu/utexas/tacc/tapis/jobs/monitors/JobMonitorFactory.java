package edu.utexas.tacc.tapis.jobs.monitors;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppTypeEnum;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** All supported monitors are instantiated using this class. */
public class JobMonitorFactory 
{
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a monitor based on the type of job and its execution environment.
     * This method either returns the appropriate monitor or throws an exception.
     * 
     * @param jobCtx job context
     * @return the monitor designated for the current job type and environment
     * @throws TapisException when no monitor is found or a network error occurs
     */
    public static JobMonitor getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
        // Extract required information from app.
        var app = jobCtx.getApp();
        var appType = app.getAppType();
        var runtime = app.getRuntime();
        
        // The result.
        JobMonitor monitor = null;
        
        // ------------------------- FORK -------------------------
        if (appType == AppTypeEnum.FORK) {
            monitor = switch (runtime) {
                case DOCKER      -> new DockerNativeMonitor(jobCtx);
                //    case SINGULARITY -> null;
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobMonitorFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (appType == AppTypeEnum.BATCH) {
            // Get the scheduler under which containers will be launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Get the monitor for each supported runtime/scheduler combination.
            monitor = switch (runtime) {
                case DOCKER      -> getBatchDockerMonitor(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityMonitor(jobCtx, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobMonitorFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_TYPE", appType, "JobMonitorFactory");
            throw new JobException(msg);
        }
        
        return monitor;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getBatchDockermonitor:                                                 */
    /* ---------------------------------------------------------------------- */
    private static JobMonitor getBatchDockerMonitor(JobExecutionContext jobCtx,
                                                    String scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker monitor. 
        JobMonitor monitor = switch (scheduler) {
            case "slurm" -> new DockerSlurmMonitor(jobCtx);
            
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(DOCKER)", 
                                             "JobMonitorFactory");
                throw new JobException(msg);
            }
        };
        
        return monitor;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityMonitor:                                            */
    /* ---------------------------------------------------------------------- */
    private static JobMonitor getBatchSingularityMonitor(JobExecutionContext jobCtx,
                                                         String scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker monitor. 
        JobMonitor monitor = switch (scheduler) {
            case "slurmX" -> null; // not implemented
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(SINGULARITY)", 
                                             "JobMonitorFactory");
                throw new JobException(msg);
            }
        };
        
        return monitor;
    }
}

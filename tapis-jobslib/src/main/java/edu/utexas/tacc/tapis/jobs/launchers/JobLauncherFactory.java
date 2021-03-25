package edu.utexas.tacc.tapis.jobs.launchers;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppTypeEnum;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** All supported launchers are instantiated using this class. */
public class JobLauncherFactory 
{
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a stager based on the type of job and its execution environment.
     * This method either returns the appropriate stager or throws an exception.
     * 
     * @param jobCtx job context
     * @return the stager designated for the current job type and environment
     * @throws TapisException when no stager is found or a network error occurs
     */
    public static JobLauncher getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
        // Extract required information from app.
        var app = jobCtx.getApp();
        var appType = app.getAppType();
        var runtime = app.getRuntime();
        
        // The result.
        JobLauncher stager = null;
        
        // ------------------------- FORK -------------------------
        if (appType == AppTypeEnum.FORK) {
            stager = switch (runtime) {
                case DOCKER      -> new DockerNativeLauncher(jobCtx);
                //    case SINGULARITY -> null;
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobLauncherFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (appType == AppTypeEnum.BATCH) {
            // Get the scheduler under which containers will be launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Get the stager for each supported runtime/scheduler combination.
            stager = switch (runtime) {
                case DOCKER      -> getBatchDockerLauncher(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityLauncher(jobCtx, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobLauncherFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_TYPE", appType, "JobExecStageFactory");
            throw new JobException(msg);
        }
        
        return stager;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getBatchDockerStager:                                                  */
    /* ---------------------------------------------------------------------- */
    private static JobLauncher getBatchDockerLauncher(JobExecutionContext jobCtx,
                                                    String scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker stager. 
        JobLauncher stager = switch (scheduler) {
            case "slurmX" -> null;
            
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(DOCKER)", 
                                             "JobLauncherFactory");
                throw new JobException(msg);
            }
        };
        
        return stager;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityStager:                                             */
    /* ---------------------------------------------------------------------- */
    private static JobLauncher getBatchSingularityLauncher(JobExecutionContext jobCtx,
                                                           String scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker stager. 
        JobLauncher stager = switch (scheduler) {
            case "slurmX" -> null; // not implemented
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(SINGULARITY)", 
                                             "JobLauncherFactory");
                throw new JobException(msg);
            }
        };
        
        return stager;
    }
}

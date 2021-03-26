package edu.utexas.tacc.tapis.jobs.stagers;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppTypeEnum;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

public final class JobExecStageFactory 
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
    public static JobExecStager getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
        // Extract required information from app.
        var app = jobCtx.getApp();
        var appType = app.getAppType();
        var runtime = app.getRuntime();
        
        // The result.
        JobExecStager stager = null;
        
        // ------------------------- FORK -------------------------
        if (appType == AppTypeEnum.FORK) {
            stager = switch (runtime) {
                case DOCKER      -> new DockerNativeStager(jobCtx);
                //    case SINGULARITY -> null;
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobExecStageFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (appType == AppTypeEnum.BATCH) {
            // Get the scheduler under which containers will be launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Doublecheck that a scheduler is assigned.
            if (scheduler == null) {
                String msg = MsgUtils.getMsg("JOBS_SYSTEM_MISSING_SCHEDULER", system.getId(), 
                                              jobCtx.getJob().getUuid());
                throw new JobException(msg);
            }
            
            // Get the stager for each supported runtime/scheduler combination.
            stager = switch (runtime) {
                case DOCKER      -> getBatchDockerStager(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityStager(jobCtx, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobExecStageFactory");
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
    private static JobExecStager getBatchDockerStager(JobExecutionContext jobCtx,
                                                      SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker stager. 
        JobExecStager stager = switch (scheduler) {
            case SLURM -> new DockerSlurmStager(jobCtx);
            
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(DOCKER)", 
                                             "JobExecStageFactory");
                throw new JobException(msg);
            }
        };
        
        return stager;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityStager:                                             */
    /* ---------------------------------------------------------------------- */
    private static JobExecStager getBatchSingularityStager(JobExecutionContext jobCtx,
                                                           SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker stager. 
        JobExecStager stager = switch (scheduler) {
            case SLURM -> null; // not implemented
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(SINGULARITY)", 
                                             "JobExecStageFactory");
                throw new JobException(msg);
            }
        };
        
        return stager;
    }
}

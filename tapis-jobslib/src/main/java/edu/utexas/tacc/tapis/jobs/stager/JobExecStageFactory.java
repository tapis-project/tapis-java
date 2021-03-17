package edu.utexas.tacc.tapis.jobs.stager;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppTypeEnum;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobExecStageFactory 
{
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
        
        // By app type, then runtime.
        if (appType == AppTypeEnum.FORK) {
            stager = switch (runtime) 
                {
                    case DOCKER      -> new JobDockerStager(jobCtx);
                //    case SINGULARITY -> null;
                    default -> {
                        String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                     "JobExecStageFactory");
                        throw new JobException(msg);
                    }
                };
        }
        else if (appType == AppTypeEnum.BATCH) {
            stager = switch (runtime) 
                {
                    case DOCKER      -> new JobDockerStager(jobCtx);
               //     case SINGULARITY -> null;
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
}

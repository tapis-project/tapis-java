package edu.utexas.tacc.tapis.jobs.cancellers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;

public class SingularityRunCanceler extends AbstractJobCanceler{
	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityStartCanceler.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunCanceler(JobExecutionContext jobCtx)     
    {
        // Create and populate the docker command.
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* launch:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
	public void cancel() throws JobException, TapisException {
    	
    	 // Best effort, no noise.
        try {
            // Get the command object.
            var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
            
            // Unconditionally remove the singularity instance container.
            removeContainer(runCmd);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SINGULARITY_CLEAN_UP_ERROR", _job.getUuid(),
                                         _job.getRemoteJobId());
            _log.error(msg, e);
        }
    	
	}
    
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TapisRunCommand runCmd)
    {
    	 // Get the command text to terminate this job's singularity instance.
        String cmd = JobExecutionUtils.SINGULARITY_RUN_KILL + _job.getRemoteJobId();
        
        // Stop the instance.
        String result = null;
        try {
            int rc = runCmd.execute(cmd);
            runCmd.logNonZeroExitCode();
            result = runCmd.getOutAsString();
        }
        catch (Exception e) {
            String execSysId = null;
            try {execSysId = _jobCtx.getExecutionSystem().getId();} catch (Exception e1) {}
            String msg = MsgUtils.getMsg("JOBS_SINGULARITY_RM_CONTAINER_ERROR", 
                                         _job.getUuid(), execSysId, result, cmd);
            _log.error(msg, e);
            return;
        }
        
        // Record the successful removal of the instance.
        if (_log.isDebugEnabled()) 
           _log.debug(MsgUtils.getMsg("JOBS_SINGULARITY_INSTANCE_REMOVED",_job.getUuid()));
    }

}

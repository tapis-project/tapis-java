package edu.utexas.tacc.tapis.jobs.cancellers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public class DockerNativeCanceler extends AbstractJobCanceler{

	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeCanceler.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DockerNativeCanceler(JobExecutionContext jobCtx)     
    {
        // Create and populate the docker command.
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* cancel:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
	public void cancel() throws JobException, TapisException {
    	
    	var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
    	// To cancel the job, remove the container from the execution system
    	removeContainer(_jobCtx.getExecutionSystem(), runCmd);
	}
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TapisSystem execSystem, TapisRunCommand runCmd)
    {
        // Get the command text for this job's container.
        String cmd = JobExecutionUtils.getDockerRmCommand(_job.getUuid());
        
        // Query the container.
        String result = null;
        try {
            int exitCode = runCmd.execute(cmd);
            if (exitCode != 0 && _log.isWarnEnabled()) 
                _log.warn(MsgUtils.getMsg("TAPIS_SSH_CMD_ERROR", cmd, 
                                          runCmd.getConnection().getHost(), 
                                          runCmd.getConnection().getUsername(), 
                                          exitCode));
            result = runCmd.getOutAsString();
        }
        catch (Exception e) {
            String cid = _job.getRemoteJobId();
            if (!StringUtils.isBlank(cid) && cid.length() >= 12) cid = cid.substring(0, 12);
            String msg = MsgUtils.getMsg("JOBS_DOCKER_RM_CONTAINER_ERROR", 
                                         _job.getUuid(), execSystem.getId(), cid, result, cmd);
            _log.error(msg, e);
        }
    }
}

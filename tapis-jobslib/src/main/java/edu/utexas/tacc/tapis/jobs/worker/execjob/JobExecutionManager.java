package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.io.ByteArrayInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.launchers.JobLauncher;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stager.JobExecStageFactory;
import edu.utexas.tacc.tapis.jobs.stager.JobExecStager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisSftp;

public final class JobExecutionManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobExecutionManager.class);
    
    // Special transfer id value indicating no files to stage.
    private static final String NO_FILE_INPUTS = "no inputs";
    
    // Job wrapper script name.
    public static final String JOB_WRAPPER_SCRIPT = "tapisjob.sh";
    public static final String JOB_ENV_FILE       = "tapisjob.env";

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final JobExecutionContext _jobCtx;
    private final Job                 _job;
    
    // The managers for different execution phases.
    private JobExecStager             _jobStager;
    private JobLauncher               _jobLauncher;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobExecutionManager(JobExecutionContext ctx)
    {
        _jobCtx = ctx;
        _job = ctx.getJob();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    public void stageJob() throws TapisException
    {
        // Assign the stager for this job.
        _jobStager = JobExecStageFactory.getInstance(_jobCtx);
        
        // Create the wrapper script.
        String wrapperScript = _jobStager.generateWrapperScript();
        
        // Create the environment variable definition file.
        String envVarFile = _jobStager.generateEnvVarFile();
        
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Install the wrapper script on the execution system.
        installExecFile(conn, wrapperScript, JOB_WRAPPER_SCRIPT, TapisSftp.RWXRWX);
        
        // Install the env variable definition file.
        installExecFile(conn, envVarFile, JOB_ENV_FILE, TapisSftp.RWRW);
    }
    
    /* ---------------------------------------------------------------------- */
    /* submitJob:                                                              */
    /* ---------------------------------------------------------------------- */
    public void submitJob() throws TapisException
    {
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Change directory to the execution directory.
        var runCmd = new TapisRunCommand(_jobCtx.getExecutionSystem(), conn);
        String cmd = "cd " + _job.getExecSystemExecDir();
        String result = runCmd.execute(cmd);
        // TODO: check result
        
        // Run the wrapper script.
        cmd = "./" + JOB_WRAPPER_SCRIPT;
        result = runCmd.execute(cmd);
        // TODO: check result
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* installExecFile:                                                       */
    /* ---------------------------------------------------------------------- */
    private void installExecFile(SSHConnection conn, String content,
                                 String fileName, int mod) 
      throws TapisException
    {
        // Put the wrapperScript text into a stream.
        var in = new ByteArrayInputStream(content.getBytes());
        
        // Calculate the destination file path.
        String filePath = _job.getExecSystemExecDir();
        if (filePath.endsWith("/")) filePath += fileName;
          else filePath += "/" + fileName;
        
        // Initialize the sftp transporter.
        var sftp = new TapisSftp(_jobCtx.getExecutionSystem(), conn);
        
        // Transfer the wrapper script.
        try {
            sftp.put(in, filePath);
            sftp.chmod(mod, filePath);
        } 
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SFTP_CMD_ERROR", 
                                         _jobCtx.getExecutionSystem().getId(),
                                         _jobCtx.getExecutionSystem().getHost(),
                                         _jobCtx.getExecutionSystem().getEffectiveUserId(),
                                         _jobCtx.getExecutionSystem().getTenant(),
                                         _job.getUuid(),
                                         filePath, e.getMessage());
            throw new JobException(msg, e);
        } 
        // Always close the channel but keep the connection open.
        finally {sftp.closeChannel();} 
    }
}

package edu.utexas.tacc.tapis.jobs.stagers;

import java.io.ByteArrayInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisSftp;

public abstract class AbstractJobExecStager 
 implements JobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobExecStager.class);
    
    // Command buffer initial capacity.
    private static final int INIT_CMD_LEN = 2048;

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Input parameters
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;
    
    // The buffer used to build command file content. 
    protected final StringBuilder       _cmd;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractJobExecStager(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
        
        // Initialize the command file text.
        _cmd = new StringBuilder(INIT_CMD_LEN);
    }


    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public void stageJob() throws TapisException 
    {
        // Create the wrapper script.
        String wrapperScript = generateWrapperScript();
        
        // Create the environment variable definition file.
        String envVarFile = generateEnvVarFile();
        
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Install the wrapper script on the execution system.
        installExecFile(conn, wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, TapisSftp.RWXRWX);
        
        // Install the env variable definition file.
        installExecFile(conn, envVarFile, JobExecutionUtils.JOB_ENV_FILE, TapisSftp.RWRW);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content.
     * 
     * @return the wrapper script content
     */
    protected abstract String generateWrapperScript() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for a environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    protected abstract String generateEnvVarFile() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* initBashScript:                                                        */
    /* ---------------------------------------------------------------------- */
    protected void initBashScript()
    {
        _cmd.append("#!/bin/bash/n/n");
        appendDescription();
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
    
    /* ---------------------------------------------------------------------- */
    /* appendDescription:                                                     */
    /* ---------------------------------------------------------------------- */
    private void appendDescription()
    {
        _cmd.append("This script was auto-generated by the Tapis Jobs Service for the purpose\n");
        _cmd.append("of running a Tapis application.  The order of execution is as follows:\n\n");
        _cmd.append("   1. Standard Tapis and user-supplied environment variables are exported.\n");
        _cmd.append("   2. The application container is run with container options, environment\n");
        _cmd.append("      variables and application parameters as specified in the Tapis job,\n");
        _cmd.append("       application and system definitions.\n");
        _cmd.append("\n");
    }
}

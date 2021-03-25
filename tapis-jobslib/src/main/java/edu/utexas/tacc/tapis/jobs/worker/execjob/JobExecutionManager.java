package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
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
    private static final String UNKNOWN_CONTAINER_ID = "<Unknown-Container-ID>";
    
    // Job wrapper script name.
    public static final String JOB_WRAPPER_SCRIPT = "tapisjob.sh";
    public static final String JOB_ENV_FILE       = "tapisjob.env";
    
    // Docker command templates.
    private static final String DOCKER_ID = "docker ps -a --no-trunc -f \"%s\" --format \"{{.ID}}\"";
    private static final String DOCKER_STATUS = "docker ps -a --no-trunc -f \"name=%s\" --format \"{{.Status}}\"";
    private static final String DOCKER_RM = "docker rm %s";

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
    /* submitJob:                                                             */
    /* ---------------------------------------------------------------------- */
    public void submitJob() throws TapisException
    {
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Get the command object.
        var runCmd = new TapisRunCommand(_jobCtx.getExecutionSystem(), conn);
        
        // Create the command that changes the directory to the execution 
        // directory and runs the wrapper script.  The directory is expressed
        // as an absolute path on the system.
        String cmd = "cd " + Paths.get(_jobCtx.getExecutionSystem().getRootDir(), 
                                        _job.getExecSystemExecDir()).toString();
        cmd += ";./" + JOB_WRAPPER_SCRIPT;
        
        // Log the command we are about to issue.
        if (_log.isInfoEnabled()) 
            _log.info(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                      _job.getUuid(), cmd));
        
        // Start the container.
        String result = runCmd.execute(cmd);

        // We expect there to be no result.
        if (!StringUtils.isBlank(result)) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_WARN", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result);
            _log.warn(msg);
        }
                
        // Get and save the container id. The container name is always the job uuid.  
        // Account for slow docker execution by retrying here.  We'll wait for 15 
        // seconds at most.
        result = null;
        final int iterations   = 4;
        final int sleepMillis  = 5000;
        for (int i = 0; i < iterations; i++) {
            // Sleep on all iterations other than the first.
            if (i != 0) try {Thread.sleep(sleepMillis);} catch (Exception e) {}
            
            // Query for the container id.
            try {result = runCmd.execute(getDockerCidCommand(_job.getUuid()));}
                catch (Exception e) {
                    int attemptsLeft = (iterations - 1) - i;
                    String msg = MsgUtils.getMsg("JOBS_GET_CID_ERROR", getClass().getSimpleName(), 
                                                 _job.getUuid(), cmd, attemptsLeft, e.getMessage());
                    _log.error(msg, e);
                    continue;
                } 
            
            // We expect the full container id to be returned.
            if (StringUtils.isBlank(result)) {
                int attemptsLeft = (iterations - 1) - i;
                String msg = MsgUtils.getMsg("JOBS_GET_CID_ERROR", getClass().getSimpleName(), 
                                             _job.getUuid(), cmd, attemptsLeft, "empty result");
                _log.error(msg);
                continue;
            }
            
            // We got an id.
            break;
        }
        
        // Save the container id or the unknown id string.
        if (StringUtils.isBlank(result)) result = UNKNOWN_CONTAINER_ID;
        _jobCtx.getJobsDao().setRemoteJobId(_job, result);
    }
    
    /* ---------------------------------------------------------------------- */
    /* monitorJob:                                                            */
    /* ---------------------------------------------------------------------- */
    public void monitorJob() throws TapisException
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDockerCidCommand:                                                   */
    /* ---------------------------------------------------------------------- */
    public String getDockerCidCommand(String containerName)
    {return String.format(DOCKER_ID, containerName);}
    
    /* ---------------------------------------------------------------------- */
    /* getDockerStatusCommand:                                                */
    /* ---------------------------------------------------------------------- */
    public String getDockerStatusCommand(String containerName)
    {return String.format(DOCKER_STATUS, containerName);}
    
    /* ---------------------------------------------------------------------- */
    /* getDockerRmCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    public String getDockerRmCommand(String containerName)
    {return String.format(DOCKER_RM, containerName);}
    
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

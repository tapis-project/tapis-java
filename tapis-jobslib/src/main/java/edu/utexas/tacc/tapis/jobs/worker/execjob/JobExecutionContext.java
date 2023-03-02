package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoveryDefinitions;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.launchers.JobLauncherFactory;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.submit.JobSharedAppCtx;
import edu.utexas.tacc.tapis.jobs.monitors.JobMonitorFactory;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg.CmdType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobStatusMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecStageFactory;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisSSH;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public final class JobExecutionContext
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobExecutionContext.class);
    
    // HTTP codes defined here so we don't reference jax-rs classes on the backend.
    public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // The different types of systems loaded in this class.
    private enum LoadSystemTypes {execution, archive, dtn}
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The job to run.
    private final Job                _job;
    private JobSharedAppCtx          _jobSharedAppCtx;
    
	// Cached dao's used throughout this file and by clients.
    private final JobsDao            _jobsDao;
    
    // Tapis resources.
    private TapisSystem              _executionSystem;
    private TapisSystem              _archiveSystem;
    private TapisSystem              _dtnSystem;
    private TapisApp                 _app;
    private LogicalQueue             _logicalQueue;
    private JobFileManager           _jobFileManager;
    private JobIOTargets             _jobIOTargets;
    private TapisSSH                 _execSysTapisSSH; // always use accessor
    private SchedulerProfile         _schedulerProfile;
    
    // Last message to be written to job record when job terminates.
    private String                   _finalMessage; 
    
    // Treat authentication errors on the initial connection attempt specially.
    private boolean                  _execSysSSHFirstAttempt = true;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobExecutionContext(Job job, JobsDao jobDao) 
    {
        // Jobs and their dao's cannot be null.
        if (job == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "JobExecutionContext", "job");
            throw new TapisRuntimeException(msg);
        }
        if (jobDao == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "JobExecutionContext", "jobDao");
            throw new TapisRuntimeException(msg);
        }
        _job = job;
        _jobSharedAppCtx = new JobSharedAppCtx(job);
        _jobsDao = jobDao;
        
        // Cross reference the job and its context.
        job.setJobCtx(this);
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getExecutionSystem:                                                    */
    /* ---------------------------------------------------------------------- */
    public TapisSystem getExecutionSystem() 
     throws TapisException 
    {
        // Load the execution system on first use.
        if (_executionSystem == null) {
            _executionSystem = loadSystemDefinition(getServiceClient(SystemsClient.class), 
                                   _job.getExecSystemId(), true, LoadSystemTypes.execution,
                                   _jobSharedAppCtx.getSharingExecSystemAppOwner());
        }
        
        return _executionSystem;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getArchiveSystem:                                                      */
    /* ---------------------------------------------------------------------- */
    public TapisSystem getArchiveSystem() 
     throws TapisException 
    {
        // Load the archive system on first use.
        if (_archiveSystem == null) {
            if (_job.getExecSystemId().equals(_job.getArchiveSystemId()))
                _archiveSystem = _executionSystem;
            if (_archiveSystem == null)    
                _archiveSystem = loadSystemDefinition(getServiceClient(SystemsClient.class), 
                                     _job.getArchiveSystemId(), false, LoadSystemTypes.archive,
                                     _jobSharedAppCtx.getSharingArchiveSystemAppOwner());
        }
        
        return _archiveSystem;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDtnSystem:                                                          */
    /* ---------------------------------------------------------------------- */
    public TapisSystem getDtnSystem() 
     throws TapisException 
    {
        // The dtn system is optional.
        if (_job.getDtnSystemId() == null) return null;
        
        // Load the execution system on first use.
        if (_dtnSystem == null) {
            _dtnSystem = loadSystemDefinition(getServiceClient(SystemsClient.class), 
                             _job.getDtnSystemId(), false, LoadSystemTypes.dtn,
                             _jobSharedAppCtx.getSharingExecSystemAppOwner());
        }
        
        return _dtnSystem;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getApp:                                                                */
    /* ---------------------------------------------------------------------- */
    public TapisApp getApp()
     throws TapisException 
    {
        // Load the execution system on first use.
        if (_app == null) 
            _app = loadAppDefinition(getServiceClient(AppsClient.class), 
                                     _job.getAppId(), _job.getAppVersion());
        
        return _app;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getLogicalQueue:                                                       */
    /* ---------------------------------------------------------------------- */
    /** This method can only throw an exception if the exec system cannot be 
     * retrieved.  If the exec system or the logical queue are already cached,
     * no exceptions will be thrown.
     * 
     * @return the job's logical queue or null
     * @throws TapisException 
     */
    public LogicalQueue getLogicalQueue() 
     throws TapisException 
    {
        // Select the job's logical queue.
        if (_logicalQueue == null) {
            String queueName = _job.getExecSystemLogicalQueue();
            var logicalQueues = getExecutionSystem().getBatchLogicalQueues();
            if (logicalQueues != null && !StringUtils.isBlank(queueName))
                for (var q : logicalQueues) 
                    if (q.getName().equals(queueName)) {
                        _logicalQueue = q;
                        break;
                    }
        }
        return _logicalQueue;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobFileManager:                                                     */
    /* ---------------------------------------------------------------------- */
    public JobFileManager getJobFileManager() 
    {
        if (_jobFileManager == null) _jobFileManager = new JobFileManager(this);
        return _jobFileManager;
    } 
    
    /* ---------------------------------------------------------------------- */
    /* getJobIOTargets:                                                       */
    /* ---------------------------------------------------------------------- */
    public JobIOTargets getJobIOTargets() 
     throws TapisException 
    {
        if (_jobIOTargets == null) 
            _jobIOTargets = new JobIOTargets(_job, getExecutionSystem(), getDtnSystem());
        return _jobIOTargets;
    } 
    
    /* ---------------------------------------------------------------------- */
    /* createDirectories:                                                     */
    /* ---------------------------------------------------------------------- */
    public void createDirectories() throws TapisException, TapisServiceConnectionException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        getJobFileManager().createDirectories();
    }
    
    /* ---------------------------------------------------------------------- */
    /* stageInputs:                                                           */
    /* ---------------------------------------------------------------------- */
    public void stageInputs() throws TapisImplException, TapisException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        getJobFileManager().stageInputs();
    }
    
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                             */
    /* ---------------------------------------------------------------------- */
    public void stageJob() throws TapisImplException, TapisException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        var stager = JobExecStageFactory.getInstance(this);
        stager.stageJob();
    }
    
    /* ---------------------------------------------------------------------- */
    /* submitJob:                                                             */
    /* ---------------------------------------------------------------------- */
    public void submitJob() throws TapisImplException, TapisException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        var launcher = JobLauncherFactory.getInstance(this);
        launcher.launch();
    }
    
    /* ---------------------------------------------------------------------- */
    /* monitorQueuedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    public void monitorQueuedJob() throws TapisImplException, TapisException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        var monitor = JobMonitorFactory.getInstance(this);
        monitor.monitorQueuedJob();
    }
    
    /* ---------------------------------------------------------------------- */
    /* monitorRunningJob:                                                     */
    /* ---------------------------------------------------------------------- */
    public void monitorRunningJob() throws TapisImplException, TapisException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        var monitor = JobMonitorFactory.getInstance(this);
        monitor.monitorRunningJob();
    }
    
    /* ---------------------------------------------------------------------- */
    /* archiveOutputs:                                                        */
    /* ---------------------------------------------------------------------- */
    public void archiveOutputs() throws TapisImplException, TapisException, TapisClientException
    {
        // Load the exec, archive and dtn systems now
        // to avoid double faults in FileManager.
        initSystems();
        getJobFileManager().archiveOutputs();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getServiceClient:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Get a new or cached Apps service client.  This can only be called after
     * the request tenant and owner have be assigned.
     * 
     * @return the client
     * @throws TapisImplException
     */
    public <T> T getServiceClient(Class<T> cls) throws TapisImplException
    {
        // Get the application client for this user@tenant.
        T client = null;
        try {
            client = ServiceClients.getInstance().getClient(
                    _job.getOwner(), _job.getTenant(), cls);
        }
        catch (Exception e) {
            var serviceName = StringUtils.removeEnd(cls.getSimpleName(), "Client");
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", serviceName, 
                                         _job.getTenant(), _job.getOwner());
            throw new TapisImplException(msg, e, HTTP_INTERNAL_SERVER_ERROR);
        }

        return client;
    }

    /* ---------------------------------------------------------------------------- */
    /* getExecSystemTapisSSH:                                                       */
    /* ---------------------------------------------------------------------------- */
    public synchronized TapisSSH getExecSystemTapisSSH() throws JobException
    {
        if (_execSysTapisSSH == null) {
            try {
                // Establish a connection to the execution system.
                _execSysTapisSSH = new TapisSSH(_executionSystem);
                _execSysTapisSSH.getConnection();
            } 
            catch (Exception e) {
                // Add the job activity to auth exceptions on first attempt only.
                if (e instanceof TapisSSHAuthException) 
                    if (_execSysSSHFirstAttempt) {
                        String activity = JobRecoveryDefinitions.BlockedJobActivity.CHECK_SYSTEMS.name();
                        RecoveryUtils.updateJobActivity(e, activity);
                        _execSysSSHFirstAttempt = false;
                    }
                   
                // Create the informative message.
                String msg = MsgUtils.getMsg("JOBS_SSH_SYSTEM_ERROR", 
                                             _executionSystem.getId(),
                                             _executionSystem.getHost(),
                                             _executionSystem.getEffectiveUserId(),
                                             _executionSystem.getTenant(),
                                             _executionSystem.getDefaultAuthnMethod().name(),
                                             _job.getUuid(),
                                             e.getMessage());
                // Always wrap the exception.
                throw new JobException(msg, e);
            }
        }
        
        // Record that we have connected to the exec system at least once.
        _execSysSSHFirstAttempt = false;
        return _execSysTapisSSH;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* usesEnvFile:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public boolean usesEnvFile()
    {
        // Get the execution system safely.
        TapisSystem execSys;
        try {execSys = getExecutionSystem();} catch (Exception e) {return true;}
        
        // Detect the runtimes that don't use a separate environment variable file.
        if (execSys.getCanRunBatch() && 
            execSys.getBatchScheduler() == SchedulerTypeEnum.SLURM)
            return false;
        
        // All others use an environment variable file.
        return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSchedulerProfile:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Return the named scheduler profile, which may be cached.  Once the profile
     * is set for a running job it cannot be changed.
     * 
     * @param profileName the target profile
     * @return null or the named profile
     */
    public SchedulerProfile getSchedulerProfile(String profileName)
     throws JobException
    {
        // Shouldn't happen.
        if (StringUtils.isBlank(profileName)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getSchedulerProfile", "profileName");
            throw new JobException(msg);
        }
        
        // Use the cached profile if it exists.
        if (_schedulerProfile == null) {
            try {_schedulerProfile = getServiceClient(SystemsClient.class).getSchedulerProfile(profileName);}
                catch (Exception e) {
                    // Not found error.
                    if ((e instanceof TapisClientException) && 
                        ((TapisClientException)e).getCode() == 404) 
                    { 
                        String msg = MsgUtils.getMsg("JOBS_SCHEDULER_PROFILE_NOT_FOUND",
                                _job.getOwner(), _job.getTenant(), profileName);
                        throw new JobException(msg, e);
                    }
                
                    // All other error cases.
                    String msg = MsgUtils.getMsg("JOBS_SCHEDULER_PROFILE_ACCESS_ERROR",
                                _job.getOwner(), _job.getTenant(), profileName, e.getMessage());
                    throw new JobException(msg, e);
                }
        }
        return _schedulerProfile;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* closeExecSystemConnection:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Close the ssh session to the execution system if one exists. */
    public synchronized void closeExecSystemConnection()
    {
        // Close the ssh session.
        if (_execSysTapisSSH != null) {
            _execSysTapisSSH.closeConnection();
            _execSysTapisSSH = null;
            
            // Log the action.
            if (_log.isInfoEnabled())
               _log.info(MsgUtils.getMsg("JOBS_SSH_CLOSE_CONN", 
                                         _job.getUuid(), _job.getExecSystemId()));
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* close:                                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Clean up after job executes. */
    public void close()
    {
        closeExecSystemConnection();
    }
    
    /* ********************************************************************** */
    /*                              Accessors                                 */
    /* ********************************************************************** */
    public Job getJob() {return _job;}
    public JobSharedAppCtx getJobSharedAppCtx() {return _jobSharedAppCtx;}
    
    public void setExecutionSystem(TapisSystem executionSystem) 
       {_executionSystem = executionSystem;}
    
    public void setApp(TapisApp app) {_app = app;}
    
    public String getFinalMessage() {return _finalMessage;}
    public void setFinalMessage(String finalMessage) {_finalMessage = finalMessage;}

    /* ********************************************************************** */
    /*                      Asynchronous Command Methods                      */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* checkCmdMsg:                                                           */
    /* ---------------------------------------------------------------------- */
    public void checkCmdMsg()
     throws JobAsyncCmdException
    {
        // See if there the job received a command message
        // and reset the job's message field to null.
        CmdMsg cmdMsg = _job.getAndSetCmdMsg();
        if (cmdMsg == null) return;
        
        // Process each message based on type.  The cancel and paused commands
        // change the job state and throw a JobAsyncCmdException to terminate
        // or postpone job processing.
        switch (cmdMsg.msgType) {
            case JOB_STATUS:  
                JobExecutionUtils.executeCmdMsg(this, (JobStatusMsg) cmdMsg);
                break;
            case JOB_CANCEL: 
                JobExecutionUtils.executeCmdMsg(this, cmdMsg, JobStatusType.CANCELLED);
                break;
            case JOB_PAUSE: 
                JobExecutionUtils.executeCmdMsg(this, cmdMsg, JobStatusType.PAUSED);
                break;
            default:
                // This should not happen.  Log it and move on.
                String msg = MsgUtils.getMsg("JOBS_CMD_MSG_ERROR", _job.getUuid(), cmdMsg.msgType.name(),
                                             cmdMsg.senderId, cmdMsg.correlationId);
                _log.error(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* checkForCancelBeforeRecovery:                                          */
    /* ---------------------------------------------------------------------- */
    /** Check for user cancellation.  This method is intended for use when the
     * job has experienced an error and also received a cancel message.  The
     * cancel message take precedence over any recovery that might be attempted.
     * This method clears the cmdmsg, so it destroys any non-cancel message that 
     * might have been sent. 
     * 
     * @return true if the job was cancelled, false otherwise.
     */
    public boolean checkForCancelBeforeRecovery()
    {
        // See if there the job received a cancel message
        // and reset the job's message field to null.
        CmdMsg cmdMsg = _job.getAndSetCmdMsg();
        if (cmdMsg == null || cmdMsg.msgType != CmdType.JOB_CANCEL) return false; 
        
        // Execute the cancel and indicate it by passing back true.
        try {JobExecutionUtils.executeCmdMsg(this, cmdMsg, JobStatusType.CANCELLED);}
            catch (JobAsyncCmdException e) {
                _log.info(MsgUtils.getMsg("JOBS_CMD_MSG_CANCELLED_BEFORE_RECOVERY", _job.getUuid()));
                return true;
            }
        
        // We didn't get the expected async exception, 
        // so we can't say cancellation succeeded.
        return false;
    }

    /* ---------------------------------------------------------------------- */
    /* getJobsDao:                                                            */
    /* ---------------------------------------------------------------------- */
    public JobsDao getJobsDao() {return _jobsDao;}

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* loadSystemDefinition:                                                        */
    /* ---------------------------------------------------------------------------- */
    private TapisSystem loadSystemDefinition(SystemsClient systemsClient,
                                             String systemId,
                                             boolean requireExecPerm,
                                             LoadSystemTypes loadType,
                                             String sharedAppCtx) 
      throws TapisException
    {
        // Load the system definition.
        TapisSystem system = null;
        final boolean returnCreds = true;
        final AuthnMethod authnMethod = null;
        final String selectAll = "allAttributes";
        final String impersonationId = null;
        try {system = systemsClient.getSystem(systemId, authnMethod, requireExecPerm, selectAll, 
                                              returnCreds, impersonationId, sharedAppCtx);} 
        catch (TapisClientException e) {
            // Look for a recoverable error in the exception chain. Recoverable
            // exceptions are those that might indicate a transient network
            // or server error, typically involving loss of connectivity.
            Throwable transferException = 
                TapisUtils.findFirstMatchingException(e, TapisConstants.CONNECTION_EXCEPTION_PREFIX);
            if (transferException != null) {
                throw new TapisServiceConnectionException(transferException.getMessage(), 
                            e, RecoveryUtils.captureServiceConnectionState(
                               systemsClient.getBasePath(), TapisConstants.SYSTEMS_SERVICE));
            }
            
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INPUT_ERROR", systemId, _job.getOwner(), 
                                          _job.getTenant(), loadType.name());
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_AUTHZ_ERROR", systemId, "READ,EXECUTE", 
                                          _job.getOwner(), _job.getTenant(), loadType.name());
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_NOT_FOUND", systemId, _job.getOwner(), 
                                          _job.getTenant(), loadType.name());
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _job.getOwner(), 
                                          _job.getTenant(), loadType.name());
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _job.getOwner(), 
                                         _job.getTenant(), loadType.name());
            throw new TapisImplException(msg, e, HTTP_INTERNAL_SERVER_ERROR);
        }
        
        // Check the enabled flag here for systems that we are definitely going to use.  The
        // DTN may or may not be used depending on what directories are specified for I/O.
        if (loadType == LoadSystemTypes.execution || loadType == LoadSystemTypes.archive) 
            JobExecutionUtils.checkSystemEnabled(system, _job);
        
        return system;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* loadAppDefinition:                                                           */
    /* ---------------------------------------------------------------------------- */
    private TapisApp loadAppDefinition(AppsClient appsClient, String appId, String appVersion)
      throws TapisException
    {
        // Load the system definition.
        TapisApp app = null;
        try {app = appsClient.getApp(appId, appVersion);} 
        catch (TapisClientException e) {
            // Look for a recoverable error in the exception chain. Recoverable
            // exceptions are those that might indicate a transient network
            // or server error, typically involving loss of connectivity.
            Throwable transferException = 
                TapisUtils.findFirstMatchingException(e, TapisConstants.CONNECTION_EXCEPTION_PREFIX);
            if (transferException != null) {
                throw new TapisServiceConnectionException(transferException.getMessage(), 
                            e, RecoveryUtils.captureServiceConnectionState(
                               appsClient.getBasePath(), TapisConstants.APPS_SERVICE));
            }
            
            // Determine why we failed.
            String appString = appId + "-" + appVersion;
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_INPUT_ERROR", appString, _job.getOwner(), 
                                          _job.getTenant());
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_AUTHZ_ERROR", appString, "READ", 
                                          _job.getOwner(), _job.getTenant());
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_NOT_FOUND", appString, _job.getOwner(), 
                                          _job.getTenant());
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_INTERNAL_ERROR", appString, _job.getOwner(), 
                                          _job.getTenant());
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String appString = appId + "-" + appVersion;
            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", appString, _job.getOwner(), 
                                         _job.getTenant());
            throw new TapisImplException(msg, e, HTTP_INTERNAL_SERVER_ERROR);
        }
        
        // Make sure the app is enabled.
        JobExecutionUtils.checkAppEnabled(app, _job);
        
        return app;
    }

    /* ---------------------------------------------------------------------- */
    /* initSystems:                                                           */
    /* ---------------------------------------------------------------------- */
    private void initSystems() throws TapisException
    {
        // Load the jobs systems to force any exceptions
        // to be surfaced at this point.
        getExecutionSystem();
        getArchiveSystem();
        getDtnSystem();
    }
}

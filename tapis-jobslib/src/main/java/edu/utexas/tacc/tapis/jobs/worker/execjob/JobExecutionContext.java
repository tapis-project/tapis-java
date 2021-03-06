package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.App;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg.CmdType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobStatusMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

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
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The job to run.
    private final Job                _job;
    
	// Cached dao's used throughout this file and by clients.
    private final JobsDao            _jobsDao;
    
    // Tapis resources.
    private TSystem                  _executionSystem;
    private TSystem                  _archiveSystem;
    private TSystem                  _dtnSystem;
    private App                      _app;
    private LogicalQueue             _logicalQueue;
    private JobFileManager           _jobFileManager;
    private JobIOTargets             _jobIOTargets;
    
    // Last message to be written to job record when job terminates.
    private String                   _finalMessage; 

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
    public TSystem getExecutionSystem() 
     throws TapisImplException, TapisServiceConnectionException 
    {
        // Load the execution system on first use.
        if (_executionSystem == null) {
            _executionSystem = loadSystemDefinition(getServiceClient(SystemsClient.class), 
                                     _job.getExecSystemId(), true, "execution");
        }
        
        return _executionSystem;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getArchiveSystem:                                                      */
    /* ---------------------------------------------------------------------- */
    public TSystem getArchiveSystem() 
     throws TapisImplException, TapisServiceConnectionException 
    {
        // Load the archive system on first use.
        if (_archiveSystem == null) {
            if (_job.getExecSystemId().equals(_job.getArchiveSystemId()))
                _archiveSystem = _executionSystem;
            if (_archiveSystem == null)    
                _archiveSystem = loadSystemDefinition(getServiceClient(SystemsClient.class), 
                                                      _job.getArchiveSystemId(), true, "archive");
        }
        
        return _archiveSystem;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDtnSystem:                                                          */
    /* ---------------------------------------------------------------------- */
    public TSystem getDtnSystem() 
     throws TapisImplException, TapisServiceConnectionException 
    {
        // The dtn system is optional.
        if (_job.getDtnSystemId() == null) return null;
        
        // Load the execution system on first use.
        if (_dtnSystem == null) {
            _dtnSystem = loadSystemDefinition(getServiceClient(SystemsClient.class), 
                                              _job.getDtnSystemId(), true, "dtn");
        }
        
        return _dtnSystem;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getApp:                                                                */
    /* ---------------------------------------------------------------------- */
    public App getApp() 
     throws TapisImplException, TapisServiceConnectionException 
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
     * @throws TapisImplException when the exec system cannot be retrieved
     * @throws TapisServiceConnectionException 
     */
    public LogicalQueue getLogicalQueue() 
     throws TapisImplException, TapisServiceConnectionException 
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
     throws TapisImplException, TapisServiceConnectionException 
    {
        if (_jobIOTargets == null) 
            _jobIOTargets = new JobIOTargets(_job, getExecutionSystem(), getDtnSystem());
        return _jobIOTargets;
    } 
    
    /* ---------------------------------------------------------------------- */
    /* createDirectories:                                                     */
    /* ---------------------------------------------------------------------- */
    public void createDirectories() throws TapisImplException, TapisServiceConnectionException
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
    
    /* ********************************************************************** */
    /*                              Accessors                                 */
    /* ********************************************************************** */
    public Job getJob() {return _job;}
    
    public void setExecutionSystem(TSystem executionSystem) 
       {_executionSystem = executionSystem;}
    
    public void setApp(App app) {_app = app;}
    
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
    private TSystem loadSystemDefinition(SystemsClient systemsClient,
                                         String systemId,
                                         boolean requireExecPerm,
                                         String  systemDesc) 
      throws TapisImplException, TapisServiceConnectionException
    {
        // Load the system definition.
        TSystem system = null;
        boolean returnCreds = true;
        AuthnMethod authnMethod = null;
        try {system = systemsClient.getSystem(systemId, returnCreds, authnMethod, requireExecPerm);} 
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
                                          _job.getTenant(), systemDesc);
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_AUTHZ_ERROR", systemId, "READ,EXECUTE", 
                                          _job.getOwner(), _job.getTenant(), systemDesc);
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_NOT_FOUND", systemId, _job.getOwner(), 
                                          _job.getTenant(), systemDesc);
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _job.getOwner(), 
                                          _job.getTenant(), systemDesc);
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _job.getOwner(), 
                                         _job.getTenant(), systemDesc);
            throw new TapisImplException(msg, e, HTTP_INTERNAL_SERVER_ERROR);
        }
        
        return system;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* loadAppDefinition:                                                           */
    /* ---------------------------------------------------------------------------- */
    private App loadAppDefinition(AppsClient appsClient, String appId, String appVersion)
      throws TapisImplException, TapisServiceConnectionException
    {
        // Load the system definition.
        App app = null;
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
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INPUT_ERROR", appString, _job.getOwner(), 
                                          _job.getTenant());
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_AUTHZ_ERROR", appString, "READ", 
                                          _job.getOwner(), _job.getTenant());
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_NOT_FOUND", appString, _job.getOwner(), 
                                          _job.getTenant());
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", appString, _job.getOwner(), 
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
        
        return app;
    }

    /* ---------------------------------------------------------------------- */
    /* initSystems:                                                           */
    /* ---------------------------------------------------------------------- */
    private void initSystems() throws TapisImplException, TapisServiceConnectionException
    {
        // Load the jobs systems to force any exceptions
        // to be surfaced at this point.
        getExecutionSystem();
        getArchiveSystem();
        getDtnSystem();
    }
}

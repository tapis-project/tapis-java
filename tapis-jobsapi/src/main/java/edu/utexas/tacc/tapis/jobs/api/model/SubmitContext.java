package edu.utexas.tacc.tapis.jobs.api.model;

import java.util.HashMap;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.App;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobParmSetMarshaller;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.JobParameterSet;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisUrlUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public final class SubmitContext 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SubmitContext.class);
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Constructor input.
    private final ReqSubmitJob       _submitReq;
    private final TapisThreadContext _threadContext;
    private final Job                _job;
    private final String             _oboUser;
    private final String             _oboTenant;
    
    // The raw sources of job information.
    private App     _app;
    private TSystem _execSystem;
    private TSystem _dtnSystem;
    private TSystem _archiveSystem;
    
    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public SubmitContext(ReqSubmitJob submitReq)
    {
        _submitReq = submitReq;
        _threadContext = TapisThreadLocal.tapisThreadContext.get();
        
        // Get the user requestor on behalf of whom we are executing.
        _oboUser   = _threadContext.getOboUser();
        _oboTenant = _threadContext.getOboTenantId();
        
        // Create the new job.
        _job = new Job();
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* initNewJob:                                                                  */
    /* ---------------------------------------------------------------------------- */
    public Job initNewJob() throws TapisImplException
    {
        // Get the app.
        assignApp();
        
        // Assign all systems needed by job.
        assignSystems();
        
        // Calculate all job arguments.
        resolveArgs();
        
        // Validate the job after all arguments are finalized.
        validateArgs();

        return _job;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignApp:                                                                   */
    /* ---------------------------------------------------------------------------- */
    public void assignApp() throws TapisImplException
    {
        // Get the application client for this user@tenant.
        AppsClient appsClient = null;
        try {
            appsClient = ServiceClients.getInstance().getClient(_oboUser, _oboTenant, AppsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Apps", _oboTenant, _oboUser);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Get the application.
        String authz = "READ,EXECUTE";
        try {_app = appsClient.getApp(_submitReq.getAppId(), _submitReq.getAppVersion(), authz);}
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INPUT_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _oboUser, _oboTenant);
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_AUTHZ_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _oboUser, _oboTenant);
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_NOT_FOUND", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _oboUser, _oboTenant);
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _oboUser, _oboTenant);
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                                         _submitReq.getAppVersion(), authz, _oboUser, _oboTenant);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Double-check!  This shouldn't happen, but it's absolutely critical that we have an app.
        if (_app == null) {
            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                                         _submitReq.getAppVersion(), authz, _oboUser, _oboTenant);
            throw new TapisImplException(msg, Status.NOT_FOUND.getStatusCode());
        }
    }

    /* ---------------------------------------------------------------------------- */
    /* assignSystems:                                                               */
    /* ---------------------------------------------------------------------------- */
    public void assignSystems() throws TapisImplException
    {
        // Get the application client for this user@tenant.
        SystemsClient systemsClient = null;
        try {
            systemsClient = ServiceClients.getInstance().getClient(_oboUser, _oboTenant, SystemsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Systems", _oboTenant, _oboUser);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Load all system definitions.
        assignExecSystem(systemsClient);
        assignArchiveSystem(systemsClient);
        assignDtnSystem(systemsClient);
    }
    
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
    public Job getJob() {return _job;}
    public App getApp() {return _app;}
    public TSystem getExecSystem() {return _execSystem;}
    public TSystem getDtnSystem() {return _dtnSystem;}
    public TSystem getArchiveSystem() {return _archiveSystem;}
    public ReqSubmitJob getSubmitReq() {return _submitReq;}

    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* assignExecSystem:                                                            */
    /* ---------------------------------------------------------------------------- */
    private void assignExecSystem(SystemsClient systemsClient) throws TapisImplException
    {
        // Use the system specified in the job submission request if it exists.
        String execSystemId = _submitReq.getExecSystemId();
        if (StringUtils.isBlank(execSystemId)) 
            execSystemId = _app.getJobAttributes().getExecSystemId();
                
        // Abort if we can't determine the exec system id.
        if (StringUtils.isBlank(execSystemId)) {
            String msg = MsgUtils.getMsg("TAPIS_JOBS_MISSING_SYSTEM", "execution");
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Load the system definition.
        boolean requireExecPerm = true;
        _execSystem = loadSystemDefinition(systemsClient, execSystemId, requireExecPerm);
        
        // Double-check!  This shouldn't happen, but it's absolutely critical that we have a system.
        if (_execSystem == null) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", execSystemId, _oboUser, _oboTenant);
            throw new TapisImplException(msg, Status.NOT_FOUND.getStatusCode());
        }
        
        // Assign the job's execution system.
        _job.setExecSystemId(_execSystem.getId());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignArchiveSystem:                                                         */
    /* ---------------------------------------------------------------------------- */
    private void assignArchiveSystem(SystemsClient systemsClient) throws TapisImplException
    {
        // Use the system specified in the job submission request if it exists.
        String archiveSystemId = _submitReq.getExecSystemId();
        if (StringUtils.isBlank(archiveSystemId)) 
            archiveSystemId = _app.getJobAttributes().getArchiveSystemId();
        
        // It's ok if no archive system is specified.
        if (StringUtils.isBlank(archiveSystemId)) return;
        
        // Load the system definition.
        boolean requireExecPerm = false;
        _archiveSystem = loadSystemDefinition(systemsClient, archiveSystemId, requireExecPerm);
        
        // Assign job's archive system.
        _job.setArchiveSystemId(_archiveSystem.getId());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignDtnSystem:                                                             */
    /* ---------------------------------------------------------------------------- */
    private void assignDtnSystem(SystemsClient systemsClient) throws TapisImplException
    {
        // See if the execution system specifies a DTN.
        if (StringUtils.isBlank(_execSystem.getDtnSystemId())) return;
        
        // Load the system definition.
        boolean requireExecPerm = false;
        _dtnSystem = loadSystemDefinition(systemsClient, _execSystem.getDtnSystemId(), 
                                          requireExecPerm);
        
        // Assign job's DTN system.
        _job.setDtnSystemId(_dtnSystem.getId());
    }

    /* ---------------------------------------------------------------------------- */
    /* loadSystemDefinition:                                                        */
    /* ---------------------------------------------------------------------------- */
    private TSystem loadSystemDefinition(SystemsClient systemsClient,
                                         String systemId, 
                                         boolean requireExecPerm) 
      throws TapisImplException
    {
        // Load the system definition.
        TSystem system = null;
        boolean returnCreds = false;
        AuthnMethod authnMethod = null;
        try {_execSystem = systemsClient.getSystem(systemId, returnCreds, authnMethod, requireExecPerm);} 
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INPUT_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), "READ,EXECUTE", _oboUser, _oboTenant);
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_AUTHZ_ERROR", systemId, _oboUser, _oboTenant);
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_NOT_FOUND", systemId, _oboUser, _oboTenant);
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _oboUser, _oboTenant);
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _oboUser, _oboTenant);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        return system;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveArgs:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private void resolveArgs() throws TapisImplException
    {
        // Combine various components that make up the job's parameterSet from
        // from the system, app and request definitions.
        resolveParameterSet();
        
//      // A mount point must also be specified.
//      String dtnMountPoint = _execSystem.getDtnMountPoint();
//      if (StringUtils.isBlank(dtnMountPoint)) {
//          String msg = MsgUtils.getMsg("SYSTEMS_DTN_NO_MOUNTPOINT", _execSystem, dtnSystemId);
//          throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
//      }
        
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveParameterSet:                                                         */
    /* ---------------------------------------------------------------------------- */
    private void resolveParameterSet()
    {
        // Copy the application's parameterSet into a shared library parameterSet.
        // Also included are any environment variables set in the system definition.
        // The returned parmSet is never null.
        var appParmSet = _app.getJobAttributes().getParameterSet();
        var sysEnv = _execSystem.getJobEnvVariables();
        var marshaller = new JobParmSetMarshaller();
        JobParameterSet marshalledParmSet = marshaller.marshalAppParmSet(appParmSet, sysEnv);
        
        // Parameters set in the job submission request have the highest precedence.
        JobParameterSet mergedParmSet = 
           marshaller.mergeParmSets(_submitReq.getParameterSet(), marshalledParmSet);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveEnvVariables:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Assign the environment variables that will be passed to the application when 
     * launched.  The environment variables are collected from the system, app and
     * request in order with increasing priority.
     */
    private void resolveEnvVariables() throws TapisImplException
    {
        // Initialize the job's environment variables map.
        var map = new HashMap<String,String>();
        
        // Populate the map in order of increasing priority starting with systems.
        var sysEnv = _execSystem.getJobEnvVariables();
        if (sysEnv != null) for (var kv : sysEnv) map.put(kv.getKey(), kv.getValue());
        
        // Get the app-specified environment variables.
        var appEnv = _app.getJobAttributes().getParameterSet().getEnvVariables();
        if (appEnv != null) for (var kv : appEnv) map.put(kv.getKey(), kv.getValue());
        
        // Get the request-specified environment variables.
        JobParameterSet reqParms = _submitReq.getParameterSet();
        if (reqParms.getEnvVariables() != null) 
            for (var kv : reqParms.getEnvVariables()) map.put(kv.getKey(), kv.getValue());
        
        // Only insert non-empty maps into the job object.
        if (!map.isEmpty()) _job.setParmEnvVariables(map);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateArgs:                                                                */
    /* ---------------------------------------------------------------------------- */
    private void validateArgs() throws TapisImplException
    {
        // Check the execute flag on the exec system.
        if (!_execSystem.getCanExec()) {
            String msg = ""; // ******** TODO
            throw new TapisImplException(msg, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Check the working directory syntax.
        
        // Check that the dtn system is defined as a dtn.
        
    }

    /* ---------------------------------------------------------------------------- */
    /* wrapForParsing:                                                              */
    /* ---------------------------------------------------------------------------- */
    private String wrapParmSetValue(String json) {return "{\"parameterSet\": " + json + "}";}
    
}

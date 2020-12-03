package edu.utexas.tacc.tapis.jobs.api.model;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.App;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
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
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* assignApp:                                                                   */
    /* ---------------------------------------------------------------------------- */
    public void assignApp() throws TapisImplException
    {
        // Get the user requestor on behalf of whom we are executing.
        var oboUser   = _threadContext.getOboUser();
        var oboTenant = _threadContext.getOboTenantId();
         
        // Get the application client for this user@tenant.
        AppsClient appsClient = null;
        try {
            appsClient = ServiceClients.getInstance().getClient(oboUser, oboTenant, AppsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Apps", oboTenant, oboUser);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Get the application.
        try {_app = appsClient.getApp(_submitReq.getAppId(), _submitReq.getAppVersion(), "READ,EXECUTE");}
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INPUT_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), "READ,EXECUTE", oboUser, oboTenant);
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_AUTHZ_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), "READ,EXECUTE", oboUser, oboTenant);
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_NOT_FOUND", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), "READ,EXECUTE", oboUser, oboTenant);
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), "READ,EXECUTE", oboUser, oboTenant);
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                                         _submitReq.getAppVersion(), "READ,EXECUTE", oboUser, oboTenant);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    /* ---------------------------------------------------------------------------- */
    /* assignSystems:                                                               */
    /* ---------------------------------------------------------------------------- */
    public void assignSystems() throws TapisImplException
    {
        // Get the user requestor on behalf of whom we are executing.
        var oboUser   = _threadContext.getOboUser();
        var oboTenant = _threadContext.getOboTenantId();
         
        // Get the application client for this user@tenant.
        SystemsClient systemsClient = null;
        try {
            systemsClient = ServiceClients.getInstance().getClient(oboUser, oboTenant, SystemsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Systems", oboTenant, oboUser);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Load all system definitions.
        assignExecSystem(systemsClient);
        assignArchiveSystem(systemsClient);
        assignDTNSystem(systemsClient);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createNewJob:                                                                */
    /* ---------------------------------------------------------------------------- */
    public Job createNewJob()
    {
        // UUID is assigned.
        Job job = new Job();
        
        // Set the systems.
        job.setExecSystemId(_execSystem.getName());
        if (_archiveSystem != null) job.setArchiveSystemId(_archiveSystem.getName());
        if (_dtnSystem != null) job.setDtnSystemId(_dtnSystem.getName());
        
        return job;
    }
    
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
    public App getApp() {return _app;}
    public void setApp(App app) {this._app = app;}
    public TSystem getExecSystem() {return _execSystem;}
    public void setExecSystem(TSystem execSystem) {this._execSystem = execSystem;}
    public TSystem getDtnSystem() {return _dtnSystem;}
    public void setDtnSystem(TSystem dtnSystem) {this._dtnSystem = dtnSystem;}
    public TSystem getArchiveSystem() {return _archiveSystem;}
    public void setArchiveSystem(TSystem archiveSystem) {this._archiveSystem = archiveSystem;}
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
        if (StringUtils.isBlank(execSystemId)) execSystemId = ""; // _app.getExecSystemId(); *** TODO
        
        // Abort if we can't determine the exec system id.
        if (StringUtils.isBlank(execSystemId)) {
            String msg = MsgUtils.getMsg("TAPIS_JOBS_MISSING_SYSTEM", "execution");
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Get the user requestor on behalf of whom we are executing.
        var oboUser   = _threadContext.getOboUser();
        var oboTenant = _threadContext.getOboTenantId();
         
        // Load the system definition.
        try {_execSystem = systemsClient.getSystem(execSystemId);} // **** FIX WHEN CLIENT IS PUSHED
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INPUT_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), "READ,EXECUTE", oboUser, oboTenant);
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_AUTHZ_ERROR", execSystemId, oboUser, oboTenant);
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_NOT_FOUND", execSystemId, oboUser, oboTenant);
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", execSystemId, oboUser, oboTenant);
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", execSystemId, oboUser, oboTenant);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignArchiveSystem:                                                         */
    /* ---------------------------------------------------------------------------- */
    private void assignArchiveSystem(SystemsClient systemsClient) throws TapisImplException
    {
        // Use the system specified in the job submission request if it exists.
        String archiveSystemId = _submitReq.getExecSystemId();
        if (StringUtils.isBlank(archiveSystemId)) archiveSystemId = ""; // _app.getArchiveSystemId(); *** TODO
        
        // It's ok if no archive system is specified.
        if (StringUtils.isBlank(archiveSystemId)) return;
            
        
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignDTNSystem:                                                             */
    /* ---------------------------------------------------------------------------- */
    private void assignDTNSystem(SystemsClient systemsClient) throws TapisImplException
    {
        // See if the execution system specifies a DTN.
        
        // See the use DTN flag is set.
    }
      
}

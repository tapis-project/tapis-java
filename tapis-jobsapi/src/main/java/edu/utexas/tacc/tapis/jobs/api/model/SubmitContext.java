package edu.utexas.tacc.tapis.jobs.api.model;

import java.nio.file.FileSystems;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.apps.client.gen.model.FileInputDefinition;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobParmSetMarshaller;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTemplateVariables;
import edu.utexas.tacc.tapis.jobs.queue.SelectQueueName;
import edu.utexas.tacc.tapis.jobs.utils.MacroResolver;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.ArgMetaSpec;
import edu.utexas.tacc.tapis.shared.model.InputSpec;
import edu.utexas.tacc.tapis.shared.model.JobParameterSet;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;
import edu.utexas.tacc.tapis.shared.model.NotificationSubscription;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.ReqMatchConstraints;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public final class SubmitContext 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SubmitContext.class);
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // The different types of systems loaded in this class.
    private enum LoadSystemTypes {execution, archive, dtn}
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Constructor input.
    private final ReqSubmitJob       _submitReq;
    private final TapisThreadContext _threadContext;
    private final Job                _job;
    
    // The raw sources of job information.
    private TapisApp     _app;
    private TapisSystem _execSystem;
    private TapisSystem _dtnSystem;
    private TapisSystem _archiveSystem;
    
    // Macro values.
    private final TreeMap<String,String> _macros = new TreeMap<String,String>();
    private MacroResolver _macroResolver;
    
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
        // Assign the owner and tenant in the request.
        // Many methods depend on assignment made here.
        assignOwnerAndTenant();
        
        // Get the app.
        assignApp();
        
        // Calculate all job arguments.
        resolveArgs();
        
        // Substitute values for tapis macros.
        assignMacros();
        
        // Assign validated values to all job fields.
        populateJob();
        
        // Return the validated and completed job.
        return _job;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignOwnerAndTenant:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** If the owner is set in the request and different from the obo user, check
     * the obo user's authorization to run jobs on behalf of the specified owner. 
     * Verify that the obo tenant is the same as the job tenant.
     * 
     * Request fields guaranteed to be assigned:
     *  - tenant
     *  - owner
     * 
     * @throws TapisImplException
     */
    public void assignOwnerAndTenant() throws TapisImplException
    {
        // Get the verified request information.
        var oboUser   = _threadContext.getOboUser();
        var oboTenant = _threadContext.getOboTenantId();
        
        // Make sure we are in the correct tenant.
        if (StringUtils.isBlank(_submitReq.getTenant())) _submitReq.setTenant(oboTenant);
        else if (!oboTenant.equals(_submitReq.getTenant())) {
            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", oboTenant, _submitReq.getTenant());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // The usual case where the requestor is the job owner. 
        if (StringUtils.isBlank(_submitReq.getOwner())) {
            _submitReq.setOwner(oboUser);
            return;
        }
        
        // No authorization needed when the oboUser is also the specified job owner.
        if (oboUser.equals(_submitReq.getOwner())) return;
        
        // Verify that the oboUser can run a job as the designated owner.
        boolean isAdmin;
        try {isAdmin = TapisUtils.isAdmin(oboUser, oboTenant);}
            catch (Exception e) {
                throw new TapisImplException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
            }
        
        // The oboUser better be an admin.
        if (!isAdmin) {
            String msg = MsgUtils.getMsg("SK_REQUESTOR_NOT_ADMIN", oboTenant, oboUser);
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignApp:                                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Load the application.  This method sets the _app field and can only be called
     * after the request owner and tenant fields have been set and validated.
     * 
     * Context fields guaranteed to be assigned:
     *  - _app 
     *  
     * @throws TapisImplException
     */
    public void assignApp() throws TapisImplException
    {
        // Get the application client for this user@tenant.
        AppsClient appsClient = null;
        try {
            appsClient = ServiceClients.getInstance().getClient(
                             _submitReq.getOwner(), _submitReq.getTenant(), AppsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Apps", _submitReq.getTenant(), _submitReq.getOwner());
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Get the application.
        final String authz = "READ,EXECUTE";
        Boolean execPerm = Boolean.TRUE;
        try {_app = appsClient.getApp(_submitReq.getAppId(), _submitReq.getAppVersion(), execPerm);}
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INPUT_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _submitReq.getOwner(), _submitReq.getTenant());
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_AUTHZ_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _submitReq.getOwner(), _submitReq.getTenant());
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_NOT_FOUND", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _submitReq.getOwner(), _submitReq.getTenant());
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _submitReq.getOwner(), _submitReq.getTenant());
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _submitReq.getOwner(), _submitReq.getTenant());
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Double-check!  This shouldn't happen, but it's absolutely critical that we have an app.
        if (_app == null) {
            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", _submitReq.getAppId(), 
                            _submitReq.getAppVersion(), authz, _submitReq.getOwner(), _submitReq.getTenant());
            throw new TapisImplException(msg, Status.NOT_FOUND.getStatusCode());
        }
        
        // Reject the job early if its application is not available.
        if (_app.getEnabled() == null || !_app.getEnabled()) 
        {
            String msg = MsgUtils.getMsg("JOBS_APP_NOT_AVAILABLE", _job.getUuid(), _app.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Check that the runtime has appropriate options selected.
        validateApp(_app);
    }

    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
    public Job getJob() {return _job;}
    public TapisApp getApp() {return _app;}
    public TapisSystem getExecSystem() {return _execSystem;}
    public TapisSystem getDtnSystem() {return _dtnSystem;}
    public TapisSystem getArchiveSystem() {return _archiveSystem;}
    public ReqSubmitJob getSubmitReq() {return _submitReq;}

    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* resolveArgs:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Resolve all request arguments by folding in argument inherited from systems
     * and applications.  Validation and macro substitution are handled in later calls.
     * 
     * @throws TapisImplException
     */
    private void resolveArgs() throws TapisImplException
    {
        // Resolve constraints before resolving systems.
        resolveConstraints();
        
        // Resolve all systems.
        resolveSystems();
        
        // Combine various components that make up the job's parameterSet from
        // from the system, app and request definitions.
        resolveParameterSet();
        
        // Resolve directory assignments.
        resolveDirectoryPathNames();
        
        // Merge tapis-defined logical queue value, which can ultimately be null.
        if (StringUtils.isBlank(_submitReq.getExecSystemLogicalQueue()))
            _submitReq.setExecSystemLogicalQueue(_app.getJobAttributes().getExecSystemLogicalQueue());
        if (StringUtils.isBlank(_submitReq.getExecSystemLogicalQueue()))
            _submitReq.setExecSystemLogicalQueue(_execSystem.getBatchDefaultLogicalQueue());
        validateExecSystemLogicalQueue(_submitReq.getExecSystemLogicalQueue());
        
        // Merge job description.
        if (StringUtils.isBlank(_submitReq.getDescription()))
            _submitReq.setDescription(_app.getJobAttributes().getDescription());
        
        // Merge archive flag.
        if (_submitReq.getArchiveOnAppError() == null)
            _submitReq.setArchiveOnAppError(_app.getJobAttributes().getArchiveOnAppError());
        if (_submitReq.getArchiveOnAppError() == null)
            _submitReq.setArchiveOnAppError(Job.DEFAULT_ARCHIVE_ON_APP_ERROR);
        
        // Merge node count.
        if (_submitReq.getNodeCount() == null)
            _submitReq.setNodeCount(_app.getJobAttributes().getNodeCount());
        if (_submitReq.getNodeCount() == null || _submitReq.getNodeCount() <= 0)
            _submitReq.setNodeCount(Job.DEFAULT_NODE_COUNT);
        
        // Merge cores per node.
        if (_submitReq.getCoresPerNode() == null)
            _submitReq.setCoresPerNode(_app.getJobAttributes().getCoresPerNode());
        if (_submitReq.getCoresPerNode() == null || _submitReq.getCoresPerNode() <= 0)
            _submitReq.setCoresPerNode(Job.DEFAULT_CORES_PER_NODE);
        
        // Merge memory MB.
        if (_submitReq.getMemoryMB() == null)
            _submitReq.setMemoryMB(_app.getJobAttributes().getMemoryMB());
        if (_submitReq.getMemoryMB() == null || _submitReq.getMemoryMB() <= 0)
            _submitReq.setMemoryMB(Job.DEFAULT_MEM_MB);
        
        // Merge max minutes.
        if (_submitReq.getMaxMinutes() == null)
            _submitReq.setMaxMinutes(_app.getJobAttributes().getMaxMinutes());
        if (_submitReq.getMaxMinutes() == null || _submitReq.getMaxMinutes() <= 0)
            _submitReq.setMaxMinutes(Job.DEFAULT_MAX_MINUTES);
        
        // Check the just assigned values against queue maximums.
        validateQueueLimits();
        
        // Merge tags, duplicates may be present at this point.
        var tags = _submitReq.getTags(); // force list creation
        if (_app.getJobAttributes().getTags() != null) tags.addAll(_app.getJobAttributes().getTags());
        
        // Merge app subscriptions into request subscription list.
        mergeSubscriptions();
        
        // Merge and validate input files.
        resolveFileInputs();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveParameterSet:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Resolve the contents of each object in the request parameter set by consulting
     * with values set in the application and system.
     * 
     * Request fields guaranteed to be assigned:
     *  - parameterSet
     * 
     * @throws TapisImplException
     */
    private void resolveParameterSet() 
     throws TapisImplException
    {
        // Copy the application's parameterSet into a shared library parameterSet.
        // Also included are any environment variables set in the system definition.
        // The returned parmSet is never null.
        var appParmSet = _app.getJobAttributes().getParameterSet();
        var sysEnv = _execSystem.getJobEnvVariables();
        var marshaller = new JobParmSetMarshaller();
        JobParameterSet appSysParmSet = marshaller.marshalAppParmSet(appParmSet, sysEnv);
        
        // Parameters set in the job submission request have the highest precedence.
        marshaller.mergeParmSets(_submitReq.getParameterSet(), appSysParmSet);
        
        // Validate parameter set components.
        validateArchiveFilters(_submitReq.getParameterSet().getArchiveFilter().getIncludes(), "includes");
        validateArchiveFilters(_submitReq.getParameterSet().getArchiveFilter().getExcludes(), "excludes");
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveConstraints:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Resolve information relating to the execution system constraints as specified
     * in the app and/or the request.  The result is to calculate the request's
     * consolidatedConstraints field with a non-null string value, possibly empty,
     * for use hereafter. 
     *  
     * Request fields guaranteed to be assigned:
     *  - consolidatedConstraints
     */  
    private void resolveConstraints()
    {
        var appConstraintList = _app.getJobAttributes().getExecSystemConstraints();
        _submitReq.consolidateConstraints(appConstraintList);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveSystems:                                                              */
    /* ---------------------------------------------------------------------------- */
    /** Resolve information relating to the execution, archive and dtn systems.  The
     * request owner and tenant must be valid.
     * 
     * Request fields guaranteed to be assigned:
     *  - dynamicExecSystem
     *  - execSystemId
     *  - archiveSystemId
     *  
     * Context fields guaranteed to be assigned if required:
     *  - _execSystem
     *  - _dtnSystem (can be null)
     *  - _archiveSsytem 
     * 
     * @throws TapisImplException
     */
    private void resolveSystems() throws TapisImplException
    {
        // --------------------- Systems Client ------------------
        // Get the system client for this user@tenant.
        SystemsClient systemsClient = getSystemsClient();
        
        // --------------------- Exec System ---------------------
        // Merge dynamic execution flag.
        if (_submitReq.getDynamicExecSystem() == null)
            _submitReq.setDynamicExecSystem(_app.getJobAttributes().getDynamicExecSystem());
        if (_submitReq.getDynamicExecSystem() == null)
            _submitReq.setDynamicExecSystem(Job.DEFAULT_DYNAMIC_EXEC_SYSTEM);
        
        // Dynamic execution system selection must be explicitly specified.
        boolean isDynamicExecSystem = _submitReq.getDynamicExecSystem();
        if (isDynamicExecSystem) resolveDynamicExecSystem(systemsClient);
          else resolveStaticExecSystem(systemsClient);
        
        // Make sure the execution system is still executable.
        if (_execSystem.getCanExec() == null || !_execSystem.getCanExec()) {
            String msg = MsgUtils.getMsg("JOBS_INVALID_EXEC_SYSTEM", _execSystem.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        // Make sure a job working directory is defined.
        if (StringUtils.isBlank(_execSystem.getJobWorkingDir())) {
            String msg = MsgUtils.getMsg("JOBS_EXEC_SYSTEM_NO_WORKING_DIR", _execSystem.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        // Make sure at least one job runtime is defined.
        if (_execSystem.getJobRuntimes() == null || _execSystem.getJobRuntimes().isEmpty()) {
            String msg = MsgUtils.getMsg("JOBS_EXEC_SYSTEM_NO_RUNTIME", _execSystem.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // --------------------- DTN System ----------------------
        // Load the dtn system if one is specified.
        if (!StringUtils.isBlank(_execSystem.getDtnSystemId())) {
            boolean requireExecPerm = false;
           _dtnSystem = loadSystemDefinition(systemsClient, _execSystem.getDtnSystemId(), 
                                             requireExecPerm, LoadSystemTypes.dtn);
           if (_dtnSystem.getIsDtn() == null || !_dtnSystem.getIsDtn()) {
               String msg = MsgUtils.getMsg("JOBS_INVALID_DTN_SYSTEM", _execSystem.getId(),
                                            _dtnSystem.getId());
               throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
           }
           // Make sure all required dtn definitions are assigned.
           if (StringUtils.isBlank(_execSystem.getDtnMountPoint())) {
               String msg = MsgUtils.getMsg("JOBS_INVALID_DTN_SYSTEM_CONFIG", _execSystem.getId(),
                                            _dtnSystem.getId(), "dtnMountPoint");
               throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
           }
           if (StringUtils.isBlank(_execSystem.getDtnMountSourcePath())) {
               String msg = MsgUtils.getMsg("JOBS_INVALID_DTN_SYSTEM_CONFIG", _execSystem.getId(),
                                            _dtnSystem.getId(), "dtnMountSourcePath");
               throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
           }
        }
        
        // --------------------- Archive System ------------------
        // Assign and load the archive system if one is specified.
        if (StringUtils.isBlank(_submitReq.getArchiveSystemId()))
            _submitReq.setArchiveSystemId(_app.getJobAttributes().getArchiveSystemId());
        
        // Only the last case has an archive system different from the exec system.
        if (StringUtils.isBlank(_submitReq.getArchiveSystemId())) {
            _submitReq.setArchiveSystemId(_submitReq.getExecSystemId());
            _archiveSystem = _execSystem;
        }
        else if (_submitReq.getArchiveSystemId().equals(_submitReq.getExecSystemId())) {
            _archiveSystem = _execSystem;
        }
        else {
            boolean requireExecPerm = false;
           _archiveSystem = loadSystemDefinition(systemsClient, _submitReq.getArchiveSystemId(), 
                                                 requireExecPerm, LoadSystemTypes.archive); 
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveStaticExecSystem:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Assign the execution system from application and/or request input.
     * 
     * Request fields guaranteed to be assigned:
     *  - execSystemId
     *  
     * Context fields guaranteed to be assigned:
     *  - _execSystem 
     * 
     * @throws TapisImplException
     */
    private void resolveStaticExecSystem(SystemsClient systemsClient) 
     throws TapisImplException
    {
        // Use the system specified in the job submission request if it exists.
        String execSystemId = _submitReq.getExecSystemId();
        if (StringUtils.isBlank(execSystemId)) {
            execSystemId = _app.getJobAttributes().getExecSystemId();
            _submitReq.setExecSystemId(execSystemId);
        }
                
        // Abort if we can't determine the exec system id.
        if (StringUtils.isBlank(execSystemId)) {
            String msg = MsgUtils.getMsg("TAPIS_JOBS_MISSING_SYSTEM", "execution");
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Load the system.
        boolean requireExecPerm = true;
        _execSystem = loadSystemDefinition(systemsClient, execSystemId, requireExecPerm, LoadSystemTypes.execution);
        
        // Double-check!  This shouldn't happen, but it's absolutely critical that we have a system.
        if (_execSystem == null) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", execSystemId, 
                                         _submitReq.getOwner(), _submitReq.getTenant(), "execution");
            throw new TapisImplException(msg, Status.NOT_FOUND.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveDynamicExecSystem:                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Choose the best system that meets the job's constraints according to the
     * selection policy in effect.
     * 
     * Request fields guaranteed to be assigned:
     *  - execSystemId
     *  
     * Context fields guaranteed to be assigned:
     *  - _execSystem 
     * 
     * @throws TapisImplException
     */
    private void resolveDynamicExecSystem(SystemsClient systemsClient) 
     throws TapisImplException
    {
        // Populate the constraint argument.
        List<TapisSystem> execSystems;
        ReqMatchConstraints constraints = new ReqMatchConstraints();
        if (_submitReq.getConsolidatedConstraints() != null)
            constraints.addMatchItem(_submitReq.getConsolidatedConstraints());
        
        // Get the candidates.
        try {execSystems = systemsClient.matchConstraints(constraints);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "Systems", "matchConstraints",
                                          _submitReq.getTenant(), _submitReq.getOwner());
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Make sure at least one system met the constraints.
        if (execSystems.isEmpty()) {
            String msg = MsgUtils.getMsg("JOBS_NO_MATCHING_SYSTEM", 
                                         _submitReq.getTenant(), _submitReq.getOwner(), 
                                         _submitReq.getConsolidatedConstraints());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // TODO: Invent optimization strategies.
        // Select the best candidate.  For now, the only selection policy is the 
        // hardcoded random policy.  This will change in future releases.
        _execSystem = execSystems.get(new Random().nextInt(execSystems.size()));
        _submitReq.setExecSystemId(_execSystem.getId());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveDirectoryPathNames:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Assign the directories of each of the systems participating in this job 
     * executions.  Note that all paths are relative to the rootDir of the system
     * on which they are defined.
     * 
     * Request fields guaranteed to be assigned:
     *  - execSystemInputDir
     *  - execSystemExecDir
     *  - execSystemOutputDir
     *  - archiveSystemDir
     * 
     */
    private void resolveDirectoryPathNames()
    {
        // Are we using a DTN?
        final boolean useDTN = _dtnSystem != null;
        
        // --------------------- Exec System ---------------------
        // The input directory is used as the basis for other exec system path names
        // if those path names are not explicitly assigned, so it must be assigned first.
        //
        // If a dtn is used, then the input directory must be relative to the dtn
        // mount point rather than the execution system's working directory.
        if (StringUtils.isBlank(_submitReq.getExecSystemInputDir()))
            _submitReq.setExecSystemInputDir(_app.getJobAttributes().getExecSystemInputDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemInputDir())) 
            if (useDTN)
                _submitReq.setExecSystemInputDir(Job.DEFAULT_DTN_SYSTEM_INPUT_DIR);
            else
                _submitReq.setExecSystemInputDir(Job.DEFAULT_EXEC_SYSTEM_INPUT_DIR);
        
        // Exec path.
        if (StringUtils.isBlank(_submitReq.getExecSystemExecDir()))
            _submitReq.setExecSystemExecDir(_app.getJobAttributes().getExecSystemExecDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemExecDir()))
            _submitReq.setExecSystemExecDir(
                Job.constructDefaultExecSystemExecDir(_submitReq.getExecSystemInputDir(), useDTN));
        
        // Output path.
        if (StringUtils.isBlank(_submitReq.getExecSystemOutputDir()))
            _submitReq.setExecSystemOutputDir(_app.getJobAttributes().getExecSystemOutputDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemOutputDir()))
            _submitReq.setExecSystemOutputDir(
                Job.constructDefaultExecSystemOutputDir(_submitReq.getExecSystemInputDir(), useDTN));
      
        // --------------------- Archive System ------------------
        // Set the archive system directory.
        if (StringUtils.isBlank(_submitReq.getArchiveSystemDir()))
            _submitReq.setArchiveSystemDir(_app.getJobAttributes().getArchiveSystemDir());
        if (StringUtils.isBlank(_submitReq.getArchiveSystemDir()))
            if (_archiveSystem == _execSystem) 
                // Leave the output in place when the exec system is also the archive system.
                _submitReq.setArchiveSystemDir(_submitReq.getExecSystemOutputDir());
            else if (useDTN)
                // When the archive system is the DTN, then we archive to the 
                // DTN's default archive directory.
                _submitReq.setArchiveSystemDir(Job.DEFAULT_DTN_SYSTEM_ARCHIVE_DIR);
            else
                // When the archive system is different from the exec system,
                // we archive to the default archive directory.
                _submitReq.setArchiveSystemDir(Job.DEFAULT_ARCHIVE_SYSTEM_DIR);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeSubscriptions:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Add any subscriptions defined in the application to the request's
     * subscription list, converting from the app type to the request type as we go.
     * 
     * Request fields guaranteed to be assigned:
     *  - subscriptions
     */
    private void mergeSubscriptions()
    {
        var subscriptions = _submitReq.getSubscriptions(); // force list creation
        if (_app.getJobAttributes().getSubscriptions() != null) 
            for (var appSub : _app.getJobAttributes().getSubscriptions()) {
                NotificationSubscription reqSub = new NotificationSubscription(appSub);
                subscriptions.add(reqSub);
            }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveFileInputs:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Merge and validate the file input specifications from the request and from
     * the application definition.    
     *
     * Request fields guaranteed to be assigned:
     *  - fileInputs
     *  
     * @throws TapisImplException 
     * 
     */
    private void resolveFileInputs() throws TapisImplException
    {
        // Get the application's input file definitions.
        List<FileInputDefinition> appInputs = _app.getJobAttributes().getFileInputDefinitions();
        if (appInputs == null) appInputs = Collections.emptyList();
        var processedAppInputNames = new HashSet<String>(1 + appInputs.size() * 2);
        
        // Get the app's input strictness setting.
        boolean strictInputs;
        if (_app.getStrictFileInputs() == null)
            strictInputs = AppsClient.DEFAULT_STRICT_FILE_INPUTS;  
          else strictInputs = _app.getStrictFileInputs();
        
        // Process each request file input.
        var reqInputs = _submitReq.getFileInputs();  // forces list creation
        for (var reqInput : reqInputs) {
            var meta = reqInput.getMeta();
            if (meta == null || StringUtils.isBlank(meta.getName())) {
                // ---------------- Unnamed Input ----------------
                // Are unnamed input file allowed by the application?
                if (strictInputs) {
                    String msg = MsgUtils.getMsg("JOBS_UNNAMED_FILE_INPUT", _app.getId(), reqInput.getSourceUrl());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Set the target path if it's not set.
                if (StringUtils.isBlank(reqInput.getTargetPath()))
                    reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
                if (StringUtils.isBlank(reqInput.getTargetPath())) {
                    String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), reqInput.getSourceUrl());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            }
            else {
                // ----------------- Named Input -----------------
                // Get the app definition for this named file input.  Iterate through
                // the list of definitions looking for a name match.
                String inputName = meta.getName();
                FileInputDefinition appInputDef = null;
                for (var def : appInputs) {
                    String defName = null;
                    if (def.getMeta() != null) defName = def.getMeta().getName();
                    if (inputName.equals(defName)) {
                        appInputDef = def;
                        break;
                    }
                }
                
                // Make sure we found a matching definition.
                if (appInputDef == null) {
                    String msg = MsgUtils.getMsg("JOBS_NO_FILE_INPUT_DEFINITION", _app.getId(), inputName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Make sure this isn't a duplicate use of the same name.
                boolean added = processedAppInputNames.add(inputName);
                if (!added) {
                    String msg = MsgUtils.getMsg("JOBS_DUPLICATE_FILE_INPUT", _app.getId(), inputName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Merge the application definition values into the request input. 
                mergeFileInput(reqInput, appInputDef);
            }
        }
        
        // Add in any inputs that are designated in the application 
        // with a sourceUrl and not already accounted for.
        for (var def : appInputs) {
            if (def.getMeta() == null) continue;  // should never happen
            String defName = def.getMeta().getName();
            if (processedAppInputNames.contains(defName)) continue;
            if (StringUtils.isBlank(def.getSourceUrl())) continue;
            
            // Only add in if required.
            Boolean required = def.getMeta().getRequired();
            if (required == null) required = AppsClient.DEFAULT_FILE_INPUT_META_REQUIRED; 
            if (!required) continue;  
            
            // Create and save the new request input object.
            var inputSpec = new InputSpec();
            mergeFileInput(inputSpec, def);
            reqInputs.add(inputSpec);
            
            // Bookkeeping.
            processedAppInputNames.add(defName);
        }
        
        // Make sure all required inputs were provided.
        for (var def : appInputs) {
            if (def.getMeta() == null) continue;  // should never happen
            Boolean required = def.getMeta().getRequired();
            if (required == null) required = AppsClient.DEFAULT_FILE_INPUT_META_REQUIRED;
            if (!required) continue;
            
            // Make sure we've processed this named input.
            String defName = def.getMeta().getName();
            if (!processedAppInputNames.contains(defName)) {
                String msg = MsgUtils.getMsg("JOBS_MISSING_FILE_INPUT", _app.getId(), defName);
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeFileInput:                                                              */
    /* ---------------------------------------------------------------------------- */
    private void mergeFileInput(InputSpec reqInput, FileInputDefinition appDef)
     throws TapisImplException
    {
        // Assign the source if necessary.
        if (StringUtils.isBlank(reqInput.getSourceUrl()))
            reqInput.setSourceUrl(appDef.getSourceUrl());
        if (StringUtils.isBlank(reqInput.getSourceUrl())) {
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Calculate the target if necessary.
        if (StringUtils.isBlank(reqInput.getTargetPath()))
            reqInput.setTargetPath(appDef.getTargetPath());
        if (StringUtils.isBlank(reqInput.getTargetPath()))
            reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
        if (StringUtils.isBlank(reqInput.getTargetPath())) {
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), reqInput.getSourceUrl());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Fill in the rest of the top-level fields.
        reqInput.setInPlace(appDef.getInPlace());
        if (reqInput.getInPlace() == null) 
            reqInput.setInPlace(AppsClient.DEFAULT_FILE_INPUT_IN_PLACE);
        
        // Set up the meta objects.
        var appMeta = appDef.getMeta();
        if (appMeta == null) return;
        ArgMetaSpec reqMeta = new ArgMetaSpec();
        reqInput.setMeta(reqMeta);
        
        // Populate the request meta object.
        reqMeta.setName(appMeta.getName());
        reqMeta.setDescription(appMeta.getDescription());
        reqMeta.setRequired(appMeta.getRequired());  
        if (reqMeta.getRequired() == null) 
            reqMeta.setRequired(AppsClient.DEFAULT_FILE_INPUT_META_REQUIRED);
        
        // Populate the key/value list.
        var appKvPairs = appMeta.getKeyValuePairs();
        if (appKvPairs == null) return;
        var reqKv = new ArrayList<KeyValuePair>();
        reqMeta.setKv(reqKv);
        for (var pair : appKvPairs) {
            var kv = new KeyValuePair();
            kv.setKey(pair.getKey());
            kv.setValue(pair.getValue());
            reqKv.add(kv);
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignMacros:                                                                */
    /* ---------------------------------------------------------------------------- */
    private void assignMacros() throws TapisImplException
    {
        // Macros can either be ground variables or derived variables.  Ground variables
        // never depend on other variables.  Macros can also be required to have an 
        // assigned value or optionally have a value.  Three cases are addressed 
        // separately below, there are no derived optional macros.
        
        // ---------- Ground, required
        // Assign required ground macros that never depend on other macros.
        _macros.put(JobTemplateVariables.JobUUID.name(),    _job.getUuid());
        _macros.put(JobTemplateVariables.Tenant.name(),     _submitReq.getTenant());
        _macros.put(JobTemplateVariables.JobOwner.name(),   _submitReq.getOwner());
        _macros.put(JobTemplateVariables.EffeciveUserId.name(), _execSystem.getEffectiveUserId());
        
        _macros.put(JobTemplateVariables.AppId.name(),      _submitReq.getAppId());
        _macros.put(JobTemplateVariables.AppVersion.name(), _submitReq.getAppVersion());
        
        _macros.put(JobTemplateVariables.ExecSystemId.name(),      _submitReq.getExecSystemId());
        _macros.put(JobTemplateVariables.ArchiveSystemId.name(),   _submitReq.getArchiveSystemId());
        _macros.put(JobTemplateVariables.DynamicExecSystem.name(), _submitReq.getDynamicExecSystem().toString());
        _macros.put(JobTemplateVariables.ArchiveOnAppError.name(), _submitReq.getArchiveOnAppError().toString());
        
        _macros.put(JobTemplateVariables.SysRootDir.name(), _execSystem.getRootDir());
        _macros.put(JobTemplateVariables.SysHost.name(), _execSystem.getHost());
        
        _macros.put(JobTemplateVariables.Nodes.name(),        _submitReq.getNodeCount().toString());
        _macros.put(JobTemplateVariables.CoresPerNode.name(), _submitReq.getCoresPerNode().toString());
        _macros.put(JobTemplateVariables.MemoryMB.name(),     _submitReq.getMemoryMB().toString());
        _macros.put(JobTemplateVariables.MaxMinutes.name(),   _submitReq.getMaxMinutes().toString());
        
        // The datetime, date and time strings all end with "Z", conforming to the ISO-8601 representation of UTC.  
        OffsetDateTime offDateTime = _job.getCreated().atOffset(ZoneOffset.UTC);
        _macros.put(JobTemplateVariables.JobCreateTimestamp.name(), _job.getCreated().toString());
        _macros.put(JobTemplateVariables.JobCreateDate.name(),      DateTimeFormatter.ISO_OFFSET_DATE.format(offDateTime));
        _macros.put(JobTemplateVariables.JobCreateTime.name(),      DateTimeFormatter.ISO_OFFSET_TIME.format(offDateTime));
        
        // ---------- Ground, optional
        if (_dtnSystem != null) {
            _macros.put(JobTemplateVariables.DtnSystemId.name(),        _execSystem.getDtnSystemId());
            _macros.put(JobTemplateVariables.DtnMountPoint.name(),      _execSystem.getDtnMountPoint());
            _macros.put(JobTemplateVariables.DtnMountSourcePath.name(), _execSystem.getDtnMountSourcePath());
        }
        
        if (!StringUtils.isBlank(_execSystem.getBucketName()))
            _macros.put(JobTemplateVariables.SysBucketName.name(), _execSystem.getBucketName());
        if (_execSystem.getBatchScheduler() != null)
            _macros.put(JobTemplateVariables.SysBatchScheduler.name(), _execSystem.getBatchScheduler().name());
        
        if (!StringUtils.isBlank(_submitReq.getExecSystemLogicalQueue())) {
            String logicalQueueName = _submitReq.getExecSystemLogicalQueue();
            _macros.put(JobTemplateVariables.ExecSystemLogicalQueue.name(), logicalQueueName);
            
            // Validation will check that the named logical queue has been defined.
            for (var q :_execSystem.getBatchLogicalQueues()) {
                if (logicalQueueName.equals(q.getName())) {
                    _macros.put(JobTemplateVariables.ExecSystemHPCQueue.name(), q.getHpcQueueName());
                    break;
                }
            }
        }
        
        // Special case where the job name may reference one or more ground macros.  Any of the previously
        // assigned macros can be referenced, subsequent macro assignments are not available.
        _submitReq.setName(replaceMacros(_submitReq.getName()));
        _macros.put(JobTemplateVariables.JobName.name(), _submitReq.getName());
        
        // ---------- Derived, required
        // Resolve values that can contain macro definitions or host functions.
        try {
            // Assign all macro values that don't need resolution before assigning any possibly dependent macro values.
            if (!MacroResolver.needsResolution(_execSystem.getJobWorkingDir()))
                _macros.put(JobTemplateVariables.JobWorkingDir.name(), _execSystem.getJobWorkingDir());
            if (!MacroResolver.needsResolution(_submitReq.getExecSystemInputDir()))
                _macros.put(JobTemplateVariables.ExecSystemInputDir.name(), _submitReq.getExecSystemInputDir());
            if (!MacroResolver.needsResolution(_submitReq.getExecSystemExecDir()))
               _macros.put(JobTemplateVariables.ExecSystemExecDir.name(), _submitReq.getExecSystemExecDir());
            if (!MacroResolver.needsResolution(_submitReq.getExecSystemOutputDir()))
                _macros.put(JobTemplateVariables.ExecSystemOutputDir.name(), _submitReq.getExecSystemOutputDir());
            if (!MacroResolver.needsResolution(_submitReq.getArchiveSystemDir()))
                _macros.put(JobTemplateVariables.ArchiveSystemDir.name(), _submitReq.getArchiveSystemDir());
            
            // Assign derived values that require resolution.  Note that we assign the execution system's working 
            // directory first since other macros can depend on it but not vice versa
            if (!_macros.containsKey(JobTemplateVariables.JobWorkingDir.name())) 
                _macros.put(JobTemplateVariables.JobWorkingDir.name(), resolveMacros(_execSystem.getJobWorkingDir()));
            
            if (!_macros.containsKey(JobTemplateVariables.ExecSystemInputDir.name())) {
                _submitReq.setExecSystemInputDir(resolveMacros(_submitReq.getExecSystemInputDir()));
                _macros.put(JobTemplateVariables.ExecSystemInputDir.name(), _submitReq.getExecSystemInputDir());    
            }
            if (!_macros.containsKey(JobTemplateVariables.ExecSystemExecDir.name())) {
                _submitReq.setExecSystemExecDir(resolveMacros(_submitReq.getExecSystemExecDir()));
                _macros.put(JobTemplateVariables.ExecSystemExecDir.name(), _submitReq.getExecSystemExecDir());
            }
            if (!_macros.containsKey(JobTemplateVariables.ExecSystemOutputDir.name())) {
                _submitReq.setExecSystemOutputDir(resolveMacros(_submitReq.getExecSystemOutputDir()));
                _macros.put(JobTemplateVariables.ExecSystemOutputDir.name(), _submitReq.getExecSystemOutputDir());
                }
            if (!_macros.containsKey(JobTemplateVariables.ArchiveSystemDir.name())) {
                _submitReq.setArchiveSystemDir(resolveMacros(_submitReq.getArchiveSystemDir()));
                _macros.put(JobTemplateVariables.ArchiveSystemDir.name(), _submitReq.getArchiveSystemDir());
            }
        } 
        catch (TapisException e) {
            throw new TapisImplException(e.getMessage(), e, Status.BAD_REQUEST.getStatusCode());
        }
        catch (Exception e) {
            throw new TapisImplException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveMacros:                                                               */
    /* ---------------------------------------------------------------------------- */
    private String resolveMacros(String text) throws TapisException
    {
        // Return the text with all the macros replaced by their resolved values.
        return getMacroResolver().resolve(text);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* replaceMacros:                                                               */
    /* ---------------------------------------------------------------------------- */
    private String replaceMacros(String text)
    {
        // Return the text with all the macros replaced by their existing values.
        return getMacroResolver().replaceMacros(text);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getMacroResolver:                                                            */
    /* ---------------------------------------------------------------------------- */
    private MacroResolver getMacroResolver()
    {
        if (_macroResolver == null) _macroResolver = new MacroResolver(_execSystem, _macros);
        return _macroResolver;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSystemsClient:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Get a new or cached Systems service client.  This can only be called after
     * the request tenant and owner have be assigned.
     * 
     * @return the client
     * @throws TapisImplException
     */
    private SystemsClient getSystemsClient() throws TapisImplException
    {
        // Get the application client for this user@tenant.
        SystemsClient systemsClient = null;
        try {
            systemsClient = ServiceClients.getInstance().getClient(
                    _submitReq.getOwner(), _submitReq.getTenant(), SystemsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Systems", 
                                         _submitReq.getTenant(), _submitReq.getOwner());
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        return systemsClient;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* loadSystemDefinition:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** Load the system but don't check for availability.  This approach allows jobs
     * to be queue to a worker who will then verify that the system is enabled and,
     * if necessary, attempt recovery.
     * 
     * @param systemsClient
     * @param systemId
     * @param requireExecPerm
     * @param systemDesc
     * @return
     * @throws TapisImplException
     */
    private TapisSystem loadSystemDefinition(SystemsClient systemsClient,
                                             String systemId, 
                                             boolean requireExecPerm,
                                             LoadSystemTypes systemType) 
      throws TapisImplException
    {
        // Load the system definition.
        TapisSystem system = null;
        boolean returnCreds = true;
        AuthnMethod authnMethod = null;
        try {system = systemsClient.getSystem(systemId, returnCreds, authnMethod, requireExecPerm);} 
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INPUT_ERROR", systemId, _submitReq.getOwner(), 
                                          _submitReq.getTenant(), systemType.name());
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_AUTHZ_ERROR", systemId, "READ,EXECUTE", 
                                          _submitReq.getOwner(), _submitReq.getTenant(), systemType.name());
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_NOT_FOUND", systemId, _submitReq.getOwner(), 
                                          _submitReq.getTenant(), systemType.name());
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _submitReq.getOwner(), 
                                          _submitReq.getTenant(), systemType.name());
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _submitReq.getOwner(), 
                                         _submitReq.getTenant(), systemType.name());
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        // Reject the job early if a required system is not available.  A DTN system
        // may be defined but not used, so we don't check its availability.
        if (system != null && 
            systemType != LoadSystemTypes.dtn &&
            (system.getEnabled() == null || !system.getEnabled())) 
        {
            String msg = MsgUtils.getMsg("JOBS_SYSTEM_NOT_AVAILABLE", _job.getUuid(), system.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        return system;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateExecSystemLogicalQueue:                                              */
    /* ---------------------------------------------------------------------------- */
    private void validateExecSystemLogicalQueue(String logicalQueueName) 
     throws TapisImplException
    {
        // Do we even need a queue?
        if (!_execSystem.getJobIsBatch()) return;
        
        // We need a queue.
        if (StringUtils.isBlank(logicalQueueName)) {
            String msg = MsgUtils.getMsg("JOBS_NO_LOGICAL_QUEUE", _app.getId(), 
                                         _execSystem.getId(), _submitReq.getTenant());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Validation will check that the named logical queue has been defined.
        for (var q : _execSystem.getBatchLogicalQueues()) 
            if (logicalQueueName.equals(q.getName())) return;

        // Queue not defined on exec system.
        String queues = null;
        for (var q : _execSystem.getBatchLogicalQueues()) {
            if (queues == null) queues = q.getName();
              else queues += ", " + q.getName();
        }
        String msg = MsgUtils.getMsg("JOBS_INVALID_LOGICAL_QUEUE", _app.getId(), 
                _execSystem.getId(), _submitReq.getTenant(), logicalQueueName, queues);
        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateQueueLimits:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** This method checks that the logical queue limits have not been exceeded in
     * the effective job request.  This method is called after the method
     * validateExecSystemLogicalQueue(), so we know the queue exists when we are
     * on a batch system.
     * 
     * @throws TapisImplException when a limit has been exceeded
     */
    private void validateQueueLimits()
     throws TapisImplException
    {
        // Does this job even use a queue?
        if (!_execSystem.getJobIsBatch()) return;
        
        // Get the queue definition which is guaranteed to exist.
        var queueName = _submitReq.getExecSystemLogicalQueue();
        LogicalQueue queue = null;
        for (var q : _execSystem.getBatchLogicalQueues()) 
            if (queueName.equals(q.getName())) {queue = q; break;}
        
        // ---------------------- Check Maximums ----------------------
        // Check the effective job request values against each queue defined limit.
        // The limits should never be null, but we verify anyway.
        Integer maxNodes = queue.getMaxNodeCount();
        if (maxNodes != null && _submitReq.getNodeCount() > maxNodes) {
            String msg = MsgUtils.getMsg("JOBS_Q_EXCEEDED_MAX_NODES", _job.getUuid(), 
                    _execSystem.getId(), queueName, maxNodes, _submitReq.getNodeCount());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }

        Integer maxCores = queue.getMaxCoresPerNode();
        if (maxCores != null && _submitReq.getCoresPerNode() > maxCores) {
            String msg = MsgUtils.getMsg("JOBS_Q_EXCEEDED_MAX_CORES", _job.getUuid(), 
                    _execSystem.getId(), queueName, maxCores, _submitReq.getCoresPerNode());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }

        Integer maxMem = queue.getMaxMemoryMB();
        if (maxMem != null && _submitReq.getMemoryMB() > maxMem) {
            String msg = MsgUtils.getMsg("JOBS_Q_EXCEEDED_MAX_MEM", _job.getUuid(), 
                    _execSystem.getId(), queueName, maxMem, _submitReq.getMemoryMB());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }

        Integer maxMinutes = queue.getMaxMinutes();
        if (maxMinutes != null && _submitReq.getMaxMinutes() > maxMinutes) {
            String msg = MsgUtils.getMsg("JOBS_Q_EXCEEDED_MAX_MINUTES", _job.getUuid(), 
                    _execSystem.getId(), queueName, maxMinutes, _submitReq.getMaxMinutes());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // ---------------------- Check Minimums ----------------------
        // Check the effective job request values against each queue defined limit.
        // The limits should never be null, but we verify anyway.
        Integer minNodes = queue.getMinNodeCount();
        if (minNodes != null && _submitReq.getNodeCount() < minNodes) {
            String msg = MsgUtils.getMsg("JOBS_Q_MIN_NODES_ERROR", _job.getUuid(), 
                    _execSystem.getId(), queueName, minNodes, _submitReq.getNodeCount());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }

        Integer minCores = queue.getMinCoresPerNode();
        if (minCores != null && _submitReq.getCoresPerNode() < minCores) {
            String msg = MsgUtils.getMsg("JOBS_Q_MIN_CORES_ERROR", _job.getUuid(), 
                    _execSystem.getId(), queueName, minCores, _submitReq.getCoresPerNode());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }

        Integer minMem = queue.getMinMemoryMB();
        if (minMem != null && _submitReq.getMemoryMB() < minMem) {
            String msg = MsgUtils.getMsg("JOBS_Q_MIN_MEM_ERROR", _job.getUuid(), 
                    _execSystem.getId(), queueName, minMem, _submitReq.getMemoryMB());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }

        Integer minMinutes = queue.getMinMinutes();
        if (minMinutes != null && _submitReq.getMaxMinutes() < minMinutes) {
            String msg = MsgUtils.getMsg("JOBS_Q_MIN_MINUTES_ERROR", _job.getUuid(), 
                    _execSystem.getId(), queueName, minMinutes, _submitReq.getMaxMinutes());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateArchiveFilters:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** Validate that each glob and regex in the filter lists can be compiled.
     * 
     * @param archiveFilter the final archive filter
     * @throws TapisImplException on invalid filter content
     */
    private void validateArchiveFilters(List<String> filters, String filterName)
     throws TapisImplException
    {
        // Compile the items in each of the filters.
        for (var f : filters) {
            if (f.startsWith(JobFileManager.REGEX_FILTER_PREFIX)) {
                try {Pattern.compile(f.substring(JobFileManager.REGEX_FILTER_PREFIX.length()));}
                    catch (Exception e) {
                        String msg = MsgUtils.getMsg("JOBS_INVALID_REGEX_FILTER", _job.getUuid(), 
                                                     filterName, f, e.getMessage());
                        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                    }
            } else {
                try {FileSystems.getDefault().getPathMatcher("glob:"+f);}
                    catch (Exception e) {
                        String msg = MsgUtils.getMsg("JOBS_INVALID_GLOB_FILTER", _job.getUuid(), 
                                                     filterName, f, e.getMessage());
                        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                    }
            }
        }
    }

    /* ---------------------------------------------------------------------------- */
    /* validateApp:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private void validateApp(TapisApp app) throws TapisImplException
    {
        // This should be checked in apps, but we double check here.
        if (app.getRuntime() == RuntimeEnum.SINGULARITY) {
            
            // Make sure one runtime execution option is chosen.
            var opts = app.getRuntimeOptions();
            boolean start = opts.contains(RuntimeOptionEnum.SINGULARITY_START);
            boolean run   = opts.contains(RuntimeOptionEnum.SINGULARITY_RUN);
            
            // Did we get conflicting information?
            if (start && run) {
                String msg = MsgUtils.getMsg("TAPIS_SINGULARITY_OPTION_CONFLICT", 
                                             _job.getUuid(), 
                                             app.getId(),
                                             RuntimeOptionEnum.SINGULARITY_START.name(),
                                             RuntimeOptionEnum.SINGULARITY_RUN.name());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            if (!(start || run)) {
                String msg = MsgUtils.getMsg("TAPIS_SINGULARITY_OPTION_MISSING", 
                                             _job.getUuid(),
                                             app.getId());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
    }

    /* ---------------------------------------------------------------------------- */
    /* populateJob:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** By the time we get here the only fields that set in the job object are those
     * set in its constructor.  This method populates the rest of the job fields 
     * after validating the values.
     * 
     * @throws TapisImplException
     */
    private void populateJob() throws TapisImplException
    {
        // The name and owner are guaranteed to be valid by this point.
        _job.setName(_submitReq.getName());
        _job.setOwner(_submitReq.getOwner());
        _job.setTenant(_submitReq.getTenant());
        _job.setDescription(replaceMacros(_submitReq.getDescription()));
        
        // Creator fields already validated.
        _job.setCreatedby(_threadContext.getOboUser());
        _job.setCreatedbyTenant(_threadContext.getOboTenantId());
        
        // TODO: JobType and JobExecClass are not implemented yet. *********
        
        // Already validated.
        _job.setAppId(_submitReq.getAppId());
        _job.setAppVersion(_submitReq.getAppVersion());
        
        // Flags already validated.
        _job.setArchiveOnAppError(_submitReq.getArchiveOnAppError());
        _job.setDynamicExecSystem(_submitReq.getDynamicExecSystem());
        
        // Exec system fields.
        _job.setExecSystemId(_submitReq.getExecSystemId());
        _job.setExecSystemInputDir(_submitReq.getExecSystemInputDir());
        _job.setExecSystemExecDir(_submitReq.getExecSystemExecDir());
        _job.setExecSystemOutputDir(_submitReq.getExecSystemOutputDir());
        
        // The logical (tapis system) queue can be null on non-batch jobs.
        _job.setExecSystemLogicalQueue(_submitReq.getExecSystemLogicalQueue());
        
        // Archive system fields.
        _job.setArchiveSystemId(_submitReq.getArchiveSystemId());
        _job.setArchiveSystemDir(_submitReq.getArchiveSystemDir());
        
        // DTN system fields.
        if (_dtnSystem != null) {
            _job.setDtnSystemId(_execSystem.getDtnSystemId());
            _job.setDtnMountPoint(_execSystem.getDtnMountPoint());
            _job.setDtnMountSourcePath(_execSystem.getDtnMountSourcePath());
        }
        
        // Assign job limits.
        _job.setNodeCount(_submitReq.getNodeCount());
        _job.setCoresPerNode(_submitReq.getCoresPerNode());
        _job.setMemoryMB(_submitReq.getMemoryMB());
        _job.setMaxMinutes(_submitReq.getMaxMinutes());
        
        // Complex types stored as json.
        _job.setFileInputs(TapisGsonUtils.getGson(false).toJson(_submitReq.getFileInputs()));
        _job.setExecSystemConstraints(_submitReq.getConsolidatedConstraints());
        _job.setSubscriptions(TapisGsonUtils.getGson(false).toJson(_submitReq.getSubscriptions()));
        
        // Add the macros to the environment variables passed to the runtime application.
        // The environment variable list is guaranteed to be non-null by this time.  The
        // populated list is then sorted and the whole parameter set serialized.
        var envVars = _submitReq.getParameterSet().getEnvVariables();
        for (var entry : _macros.entrySet()) {
            var kv = new KeyValuePair();
            kv.setKey(Job.TAPIS_ENV_VAR_PREFIX + entry.getKey());
            kv.setValue(entry.getValue());
            envVars.add(kv);
        }
        envVars.sort(new KeyValuePairComparator());
        _job.setParameterSet(TapisGsonUtils.getGson(false).toJson(_submitReq.getParameterSet()));
            
        // Tags.
        var tags = new TreeSet<String>();
        tags.addAll(_submitReq.getTags());
        _job.setTags(tags);
        
        // Assign tapisQueue now that the job object is completely initialized.
        _job.setTapisQueue(new SelectQueueName().select(_job));
    }
    
    /* **************************************************************************** */
    /*                        KeyValuePairComparator class                          */
    /* **************************************************************************** */
    private static final class KeyValuePairComparator
     implements Comparator<KeyValuePair>
    {
        @Override
        public int compare(KeyValuePair o1, KeyValuePair o2) 
        {return o1.getKey().compareToIgnoreCase(o2.getKey());}
    }
}

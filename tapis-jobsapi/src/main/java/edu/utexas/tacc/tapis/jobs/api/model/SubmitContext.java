package edu.utexas.tacc.tapis.jobs.api.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.App;
import edu.utexas.tacc.tapis.apps.client.gen.model.FileInputDefinition;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobParmSetMarshaller;
import edu.utexas.tacc.tapis.jobs.model.Job;
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
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.ReqMatchConstraints;
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
        
        // Substitute macro values.
        
        
        // Validate the job after all arguments are finalized.
        validateArgs();

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
        String authz = "READ,EXECUTE";
        try {_app = appsClient.getApp(_submitReq.getAppId(), _submitReq.getAppVersion(), authz);}
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
    /* resolveArgs:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Resolve all request arguments by folding in argument inherited from systems
     * and applications.  Validation and macro substitution are handled in later calls.
     * 
     * @throws TapisImplException
     */
    private void resolveArgs() throws TapisImplException
    {
        // Combine various components that make up the job's parameterSet from
        // from the system, app and request definitions.
        resolveParameterSet();
        
        // Resolve constraints before resolving systems.
        resolveConstraints();
        
        // Resolve all systems.
        resolveSystems();
        
        // Resolve directory assignments.
        resolveDirectoryPathNames();
        
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
        if (_submitReq.getNodeCount() == null)
            _submitReq.setNodeCount(Job.DEFAULT_NODE_COUNT);
        
        // Merge cores per node.
        if (_submitReq.getCoresPerNode() == null)
            _submitReq.setCoresPerNode(_app.getJobAttributes().getCoresPerNode());
        if (_submitReq.getCoresPerNode() == null)
            _submitReq.setCoresPerNode(Job.DEFAULT_CORES_PER_NODE);
        
        // Merge memory MB.
        if (_submitReq.getMemoryMB() == null)
            _submitReq.setMemoryMB(_app.getJobAttributes().getMemoryMB());
        if (_submitReq.getMemoryMB() == null)
            _submitReq.setMemoryMB(Job.DEFAULT_MEM_MB);
        
        // Merge max minutes.
        if (_submitReq.getMaxMinutes() == null)
            _submitReq.setMaxMinutes(_app.getJobAttributes().getMaxMinutes());
        if (_submitReq.getMaxMinutes() == null)
            _submitReq.setMaxMinutes(Job.DEFAULT_MAX_MINUTES);
        
        // Merge tags, duplicates may be present at this point.
        var tags = _submitReq.getTags(); // force list creation
        if (_app.getTags() != null) tags.addAll(_app.getTags());
        
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
        
        // --------------------- DTN System ----------------------
        // Load the dtn system if one is specified.
        if (!StringUtils.isBlank(_execSystem.getDtnSystemId())) {
            boolean requireExecPerm = false;
           _dtnSystem = loadSystemDefinition(systemsClient, _execSystem.getDtnSystemId(), 
                                             requireExecPerm, "DTN"); 
        }
        
        // --------------------- Archive System ------------------
        // Load the archive system if one is specified.
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
                                                 requireExecPerm, "archive"); 
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
        _execSystem = loadSystemDefinition(systemsClient, execSystemId, requireExecPerm, "execution");
        
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
        // Get the candidates.
        List<TSystem> execSystems;
        ReqMatchConstraints constraints = new ReqMatchConstraints();
        constraints.addMatchItem(_submitReq.getConsolidatedConstraints());
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
        // --------------------- Exec System ---------------------
        // The input directory is used as the basis for other exec system path names
        // if those path names are not explicitly assigned, so it must be assigned first.
        //
        // If a dtn is used, then the input directory must be relative to the dtn
        // mount point rather than the execution system's working directory.
        if (StringUtils.isBlank(_submitReq.getExecSystemInputDir()))
            _submitReq.setExecSystemInputDir(_app.getJobAttributes().getExecSystemInputDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemInputDir())) 
            if (_dtnSystem == null)
                _submitReq.setExecSystemInputDir(Job.DEFAULT_EXEC_SYSTEM_INPUT_DIR);
            else
                _submitReq.setExecSystemInputDir(Job.DEFAULT_DTN_SYSTEM_INPUT_DIR);
        
        // Exec path.
        if (StringUtils.isBlank(_submitReq.getExecSystemExecDir()))
            _submitReq.setExecSystemExecDir(_app.getJobAttributes().getExecSystemExecDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemExecDir()))
            _submitReq.setExecSystemExecDir(
                Job.constructDefaultExecSystemExecDir(_submitReq.getExecSystemInputDir()));
        
        // Output path.
        if (StringUtils.isBlank(_submitReq.getExecSystemOutputDir()))
            _submitReq.setExecSystemOutputDir(_app.getJobAttributes().getExecSystemOutputDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemOutputDir()))
            _submitReq.setExecSystemOutputDir(
                Job.constructDefaultExecSystemOutputDir(_submitReq.getExecSystemInputDir()));
      
        // --------------------- Archive System ------------------
        // Set the archive system directory.
        if (StringUtils.isBlank(_submitReq.getArchiveSystemDir()))
            _submitReq.setArchiveSystemDir(_app.getJobAttributes().getArchiveSystemDir());
        if (StringUtils.isBlank(_submitReq.getArchiveSystemDir()))
            if (_archiveSystem == _execSystem) 
                // Leave the output in place when the exec system is also the archive system.
                _submitReq.setArchiveSystemDir(_submitReq.getExecSystemOutputDir());
            else if (_dtnSystem == null)
                // When the archive system is different from the exec system,
                // we archive to the default archive directory.
                _submitReq.setArchiveSystemDir(Job.DEFAULT_ARCHIVE_SYSTEM_DIR);
            else
                // When the archive system is the DTN, then we archive to the 
                // DTN's default archive directory.
                _submitReq.setArchiveSystemDir(Job.DEFAULT_DTN_SYSTEM_ARCHIVE_DIR);
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
            strictInputs = Job.DEFAULT_STRICT_FILE_INPUTS;  // TODO: ********** TEMP, wait for apps constant
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
            if (required == null || !required) continue;  // TODO:  ****** assumes default is falso
            
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
            if (required == null || !required) continue;  // TODO:  ****** assumes default is falso
            
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
        if (reqInput.getInPlace() == null) reqInput.setInPlace(Boolean.FALSE); // TODO:  ****** assumes default is falso
        
        // Set up the meta objects.
        var appMeta = appDef.getMeta();
        if (appMeta == null) return;
        ArgMetaSpec reqMeta = new ArgMetaSpec();
        reqInput.setMeta(reqMeta);
        
        // Populate the request meta object.
        reqMeta.setName(appMeta.getName());
        reqMeta.setDescription(appMeta.getDescription());
        reqMeta.setRequired(appMeta.getRequired());  // TODO:  ****** assumes default is falso
        if (reqMeta.getRequired() == null) reqMeta.setRequired(Boolean.FALSE);
        
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
    private TSystem loadSystemDefinition(SystemsClient systemsClient,
                                         String systemId, 
                                         boolean requireExecPerm,
                                         String  systemDesc) 
      throws TapisImplException
    {
        // Load the system definition.
        TSystem system = null;
        boolean returnCreds = true;
        AuthnMethod authnMethod = null;
        try {system = systemsClient.getSystem(systemId, returnCreds, authnMethod, requireExecPerm);} 
        catch (TapisClientException e) {
            // Determine why we failed.
            String msg;
            switch (e.getCode()) {
                case 400:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INPUT_ERROR", systemId, _submitReq.getOwner(), 
                                          _submitReq.getTenant(), systemDesc);
                break;
                
                case 401:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_AUTHZ_ERROR", systemId, "READ,EXECUTE", 
                                          _submitReq.getOwner(), _submitReq.getTenant(), systemDesc);
                break;
                
                case 404:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_NOT_FOUND", systemId, _submitReq.getOwner(), 
                                          _submitReq.getTenant(), systemDesc);
                break;
                
                default:
                    msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _submitReq.getOwner(), 
                                          _submitReq.getTenant(), systemDesc);
            }
            throw new TapisImplException(msg, e, e.getCode());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SYSCLIENT_INTERNAL_ERROR", systemId, _submitReq.getOwner(), 
                                         _submitReq.getTenant(), systemDesc);
            throw new TapisImplException(msg, e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        
        return system;
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

}

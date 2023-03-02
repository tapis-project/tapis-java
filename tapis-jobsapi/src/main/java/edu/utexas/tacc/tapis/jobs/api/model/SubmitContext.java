package edu.utexas.tacc.tapis.jobs.api.model;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
import edu.utexas.tacc.tapis.apps.client.gen.model.AppFileInput;
import edu.utexas.tacc.tapis.apps.client.gen.model.AppFileInputArray;
import edu.utexas.tacc.tapis.apps.client.gen.model.FileInputModeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe;
import edu.utexas.tacc.tapis.jobs.api.utils.JobParmSetMarshaller;
import edu.utexas.tacc.tapis.jobs.api.utils.JobParmSetMarshaller.ArgTypeEnum;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTemplateVariables;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.model.submit.JobArgSpec;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInputArray;
import edu.utexas.tacc.tapis.jobs.model.submit.JobParameterSet;
import edu.utexas.tacc.tapis.jobs.model.submit.JobSharedAppCtx;
import edu.utexas.tacc.tapis.jobs.model.submit.JobSharedAppCtx.JobSharedAppCtxEnum;
import edu.utexas.tacc.tapis.jobs.queue.SelectQueueName;
import edu.utexas.tacc.tapis.jobs.utils.MacroResolver;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.uri.TapisLocalUrl;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;
import edu.utexas.tacc.tapis.shared.utils.PathSanitizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.ReqMatchConstraints;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/** This class orchestrates the job submission process, which includes incorporating
 * and validating file, application and system information.  
 * 
 * Shared Application Context 
 * --------------------------
 * When the application specified in the job request is retrieved and its sharedAppCtx
 * flag is set, then the job will execute in a shared application context.  This means
 * that zero or more of the application fields listed below will have their Tapis 
 * authorization checking suspended during job execution. 
 * 
 * 1. execSystemId
 * 2. execSystemExecDir
 * 3. execSystemInputDir
 * 4. execSystemOutputDir
 * 5. archiveSystemId
 * 6. archiveSystemDir
 * 7. fileInputs sourceUrl
 * 8. fileInputs targetPath
 * 
 * The procedure for determining whether authorization is suspended on each field
 * is as follows:
 * 
 *  1) If the field is not set in the application, authorization will not be suspended.
 *  2) Otherwise, if the field's value is not changed in job request, then authorization 
 *     will be suspended.
 *     
 * This procedure guarantees that for the fields listed above, authorization will only
 * be suspended if they are assigned a value in the application definition and that 
 * value is not changed by the job request.
 *      
 * @author rcardone
 */
public final class SubmitContext 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SubmitContext.class);
    private static final int MAX_INPUT_NAME_LEN = 80;
    
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
    private TapisApp    _app;
    private TapisSystem _execSystem;
    private TapisSystem _dtnSystem;
    private TapisSystem _archiveSystem;
    
    // Shared application context is initialized after the application is loaded.
    private JobSharedAppCtx _sharedAppCtx;
    
    // Macro values.  The resolver is configured ONLY for the execution system.
    // If you need to access the archive system, use a different resolver.
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
        
        // Canonicalize paths.
        finalizePaths();
        
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
        
        // Always establish our shared application context.
        _sharedAppCtx = new JobSharedAppCtx(_app);
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
        
        // Resolve all systems and their sharing attributes.
        resolveSystems();
        
        // Resolve job type.
        resolveJobType();
        
        // Combine various components that make up the job's parameterSet from
        // from the system, app and request definitions.
        resolveParameterSet();
        
        // Resolve directory assignments and their sharing attributes.
        resolveDirectoryPathNames();
        
        // Resolve MPI and command prefix values.
        resolveMpiAndCmdPrefix();
        
        // Merge tapis-defined logical queue value only when we are running in batch mode.
        if (JobType.BATCH.name().equals(_submitReq.getJobType())) 
        {
            if (StringUtils.isBlank(_submitReq.getExecSystemLogicalQueue()))
                _submitReq.setExecSystemLogicalQueue(_app.getJobAttributes().getExecSystemLogicalQueue());
            if (StringUtils.isBlank(_submitReq.getExecSystemLogicalQueue()))
                _submitReq.setExecSystemLogicalQueue(_execSystem.getBatchDefaultLogicalQueue());
            var hpcQueueName = validateExecSystemLogicalQueue(_submitReq.getExecSystemLogicalQueue());
            if (hpcQueueName != null) _submitReq.setHpcQueueName(hpcQueueName);
        }
        
        // Merge job description.
        if (StringUtils.isBlank(_submitReq.getDescription()))
            _submitReq.setDescription(_app.getJobAttributes().getDescription());
        if (StringUtils.isBlank(_submitReq.getDescription()))
            _submitReq.setDescription(getDefaultDescription());
        
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
        
        // Assign and validate optional notes.
        validateNotes();
        
        // Merge and validate input files. Array inputs should be processed 
        // after the single inputs.  Array inputs get expanded into single 
        // input objects and added to the inputs list.
        resolveFileInputs();
        resolveFileInputArrays();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getDefaultDescription:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** This method should only be called after the tenant, owner, and application
     * have been resolved.
     * 
     * @return a default job descriptions
     */
    private String getDefaultDescription()
    {
        return _app.getId() + "-" + _app.getVersion() + " submitted by " + 
               _submitReq.getOwner() + "@" + _submitReq.getTenant(); 
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
        // Get convenient access to the two parameter sets.
        // Make sure the request set it initialized.
        var appParmSet = _app.getJobAttributes().getParameterSet();
        var reqParmSet = _submitReq.getParameterSet(); // fully initialized
        
        // Merge the argSpecs.
        var marshaller = new JobParmSetMarshaller();
        marshaller.mergeArgSpecList(reqParmSet.getAppArgs(), appParmSet.getAppArgs(), ArgTypeEnum.APP_ARGS);
        marshaller.mergeArgSpecList(reqParmSet.getContainerArgs(), appParmSet.getContainerArgs(), ArgTypeEnum.CONTAINER_ARGS);
        marshaller.mergeArgSpecList(reqParmSet.getSchedulerOptions(), appParmSet.getSchedulerOptions(), ArgTypeEnum.SCHEDULER_OPTIONS);
        marshaller.mergeTapisProfileFromSystem(reqParmSet.getSchedulerOptions(), _execSystem.getBatchSchedulerProfile());
        
        // Copy the remaining application's parameterSet fields into a jobs 
        // parameterSet. Also include any environment variables set in the 
        // system definition.  The returned parmSet is never null, but may
        // contain uninitialized (null) components.
        var sysEnv = _execSystem.getJobEnvVariables();
        JobParameterSet appSysParmSet = marshaller.marshalAppParmSet(appParmSet, sysEnv);
        
        // Parameters set in the job submission request have the highest precedence.
        marshaller.mergeNonArgParms(reqParmSet, appSysParmSet);
        
        // Validate parameter set components.
        validateArchiveFilters(reqParmSet.getArchiveFilter().getIncludes(), "includes");
        validateArchiveFilters(reqParmSet.getArchiveFilter().getExcludes(), "excludes");
        validateSchedulerProfile(reqParmSet.getSchedulerOptions());
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
        // The _execSystem field is always filled in after this code block.
        // Static system selection includes calculating the sharing attribute.
        boolean isDynamicExecSystem = _submitReq.getDynamicExecSystem();
        if (isDynamicExecSystem) resolveDynamicExecSystem(systemsClient);
          else resolveStaticExecSystem(systemsClient);
        
        // Make sure the execution system is still executable.
        if (_execSystem.getCanExec() == null || !_execSystem.getCanExec()) {
            String msg = MsgUtils.getMsg("JOBS_INVALID_EXEC_SYSTEM", _execSystem.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        // Make sure the execution system allows command prefixes if a command prefix is present on
        // the job or application
        if (_execSystem.getEnableCmdPrefix() == null || !_execSystem.getEnableCmdPrefix()) {
            if(!StringUtils.isBlank(_submitReq.getCmdPrefix()) ||
                    !StringUtils.isBlank(_app.getJobAttributes().getCmdPrefix()) ) {
                String msg = MsgUtils.getMsg("JOBS_CMD_PREFIX_NOT_ENABLED_FOR_SYSTEM", _execSystem.getId());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
        // Make sure a job working directory is defined.
        if (StringUtils.isBlank(_execSystem.getJobWorkingDir())) {
            String msg = MsgUtils.getMsg("JOBS_EXEC_SYSTEM_NO_WORKING_DIR", _execSystem.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        // Make sure the working directory is clean.
        sanitizePath(_execSystem.getJobWorkingDir(), "jobWorkingDir");
        // Make sure at least one job runtime is defined.
        if (_execSystem.getJobRuntimes() == null || _execSystem.getJobRuntimes().isEmpty()) {
            String msg = MsgUtils.getMsg("JOBS_EXEC_SYSTEM_NO_RUNTIME", _execSystem.getId());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // --------------------- DTN System ----------------------
        // Load the dtn system if one is specified.  Note that the DTN is defined in the
        // execution system definition so it inherits the sharing attribute of that system.
        if (!StringUtils.isBlank(_execSystem.getDtnSystemId())) {
            boolean requireExecPerm = false;
           _dtnSystem = loadSystemDefinition(systemsClient, _execSystem.getDtnSystemId(), 
                                             requireExecPerm, LoadSystemTypes.dtn, 
                                             _sharedAppCtx.getSharingExecSystemAppOwner()); // exec system sharing
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
        
        // Assign the default archive system if it's still blank.
        if (StringUtils.isBlank(_submitReq.getArchiveSystemId())) 
            _submitReq.setArchiveSystemId(_submitReq.getExecSystemId());
        
        // Determine the shared application context attribute.  By this time
        // the request archive system has been assigned, though that system
        // may not be loaded yet.
        _sharedAppCtx.calcArchiveSystemId(_submitReq.getArchiveSystemId(), 
                                          _app.getJobAttributes().getArchiveSystemId(),
                                          _submitReq.getExecSystemId());
                
        // Assign the archive system object if it's the same as the execution system.
        if (_submitReq.getArchiveSystemId().equals(_submitReq.getExecSystemId()))
            _archiveSystem = _execSystem;
        else {
            // Load the archive system if it's different from the execution system,
            // which is the same as saying that it hasn't been assigned yet.
            boolean requireExecPerm = false;
           _archiveSystem = loadSystemDefinition(systemsClient, _submitReq.getArchiveSystemId(), 
                                                 requireExecPerm, LoadSystemTypes.archive,
                                                 _sharedAppCtx.getSharingArchiveSystemAppOwner()); 
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
        
        // Determine the shared application context attribute.
        _sharedAppCtx.calcExecSystemId(execSystemId, _app.getJobAttributes().getExecSystemId());
                
        // Load the system.
        boolean requireExecPerm = true;
        _execSystem = loadSystemDefinition(systemsClient, execSystemId, requireExecPerm, 
                                           LoadSystemTypes.execution, _sharedAppCtx.getSharingExecSystemAppOwner());
        
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
    /* resolveJobType:                                                              */
    /* ---------------------------------------------------------------------------- */
    /** Resolve the job type using the request, application and execution system if
     * necessary.  The job type is always valid when this method completes normally.
     * 
     * @throws TapisImplException invalid job type
     */
    private void resolveJobType() throws TapisImplException
    {
        // Explicitly assigned in app.
        if (StringUtils.isBlank(_submitReq.getJobType())) {
            // Check app assignment if it exists.
            var appJobType = _app.getJobType();
            if (appJobType != null) {
                try {JobType.valueOf(appJobType.name());}
                    catch (Exception e) {
                        String msg = MsgUtils.getMsg("JOBS_INVALID_APP_JOBTYPE", 
                                       _app.getId(), _app.getVersion(), appJobType);
                        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                    }
                _submitReq.setJobType(appJobType.name());
            }
        } 
        
        // Automatically assign according to exec system configuration. 
        if (StringUtils.isBlank(_submitReq.getJobType())) {
            var canRunBatch = _execSystem.getCanRunBatch();
            if (canRunBatch == null) canRunBatch = Boolean.FALSE; // default
            if (canRunBatch) _submitReq.setJobType(JobType.BATCH.name());
              else _submitReq.setJobType(JobType.FORK.name());
        }
        
        // The submitReq's job type is not null if we get here,
        // but we still need to validate its value in some cases.
        try {JobType.valueOf(_submitReq.getJobType());}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_INVALID_JOBTYPE", 
                                         _submitReq.getJobType());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
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
     * @throws TapisImplException 
     * 
     */
    private void resolveDirectoryPathNames() throws TapisImplException
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
            if (useDTN)
                _submitReq.setExecSystemExecDir(Job.DEFAULT_DTN_SYSTEM_EXEC_DIR);
            else
                _submitReq.setExecSystemExecDir(Job.DEFAULT_EXEC_SYSTEM_EXEC_DIR);
        
        // Output path.
        if (StringUtils.isBlank(_submitReq.getExecSystemOutputDir()))
            _submitReq.setExecSystemOutputDir(_app.getJobAttributes().getExecSystemOutputDir());
        if (StringUtils.isBlank(_submitReq.getExecSystemOutputDir()))
            if (useDTN)
                _submitReq.setExecSystemOutputDir(Job.DEFAULT_DTN_SYSTEM_OUTPUT_DIR);
            else
                _submitReq.setExecSystemOutputDir(Job.DEFAULT_EXEC_SYSTEM_OUTPUT_DIR);
      
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
        
        // Assign the sharing attributes for all four directories.
        calculateDirectorySharing(useDTN);
        
        // Detect ".." segments in path early. 
        sanitizeDirectoryPathnames();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calculateDirectorySharing:                                                   */
    /* ---------------------------------------------------------------------------- */
    private void calculateDirectorySharing(boolean useDTN)
    {
        // Don't waste a lot time...
        if (!_sharedAppCtx.isSharingEnabled()) return;
        
        // Are we accessing the input directory in a shared context?
        var defaultDir = useDTN ? Job.DEFAULT_DTN_SYSTEM_INPUT_DIR :
                                  Job.DEFAULT_EXEC_SYSTEM_INPUT_DIR;
        _sharedAppCtx.calcExecDirSharing(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_INPUT_DIR,
                                         _submitReq.getExecSystemInputDir(),
                                         _app.getJobAttributes().getExecSystemInputDir(), 
                                         defaultDir);

        // Are we accessing the exec directory in a shared context?
        defaultDir = useDTN ? Job.DEFAULT_DTN_SYSTEM_EXEC_DIR :
                              Job.DEFAULT_EXEC_SYSTEM_EXEC_DIR;
        _sharedAppCtx.calcExecDirSharing(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_EXEC_DIR,
                                         _submitReq.getExecSystemExecDir(),
                                         _app.getJobAttributes().getExecSystemExecDir(), 
                                         defaultDir);
        
        // Are we accessing the output directory in a shared context?
        defaultDir = useDTN ? Job.DEFAULT_DTN_SYSTEM_OUTPUT_DIR :
                              Job.DEFAULT_EXEC_SYSTEM_OUTPUT_DIR;
        _sharedAppCtx.calcExecDirSharing(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_OUTPUT_DIR,
                                         _submitReq.getExecSystemOutputDir(),
                                         _app.getJobAttributes().getExecSystemOutputDir(), 
                                         defaultDir);

        // Are we accessing the archive directory in a shared context?
        defaultDir = useDTN ? Job.DEFAULT_DTN_SYSTEM_ARCHIVE_DIR :
                              Job.DEFAULT_ARCHIVE_SYSTEM_DIR;
        _sharedAppCtx.calcArchiveDirSharing(_submitReq.getArchiveSystemDir(),
                                            _app.getJobAttributes().getArchiveSystemDir(),
                                            _submitReq.getArchiveSystemId(),
                                            _app.getJobAttributes().getArchiveSystemId(),
                                            _submitReq.getExecSystemId(),
                                            defaultDir);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveMpiAndCmdPrefix:                                                      */
    /* ---------------------------------------------------------------------------- */
    private void resolveMpiAndCmdPrefix() throws TapisImplException
    {
        // Determine whether MPI is indicated.
        if (_submitReq.getIsMpi() == null) {
            if (_app.getJobAttributes().getIsMpi() != null)
                _submitReq.setIsMpi(_app.getJobAttributes().getIsMpi());
            else 
                _submitReq.setIsMpi(Boolean.FALSE);
        }
        
        // Set the MPI command or leave as null.
        if (StringUtils.isBlank(_submitReq.getMpiCmd())) {
            if (StringUtils.isNotBlank(_app.getJobAttributes().getMpiCmd()))
                _submitReq.setMpiCmd(_app.getJobAttributes().getMpiCmd());
            else if (StringUtils.isNotBlank(_execSystem.getMpiCmd()))
                _submitReq.setMpiCmd(_execSystem.getMpiCmd());
        }
        
        // Set the command prefix or leave as null.
        if (StringUtils.isBlank(_submitReq.getCmdPrefix())) 
            if (StringUtils.isNotBlank(_app.getJobAttributes().getCmdPrefix()))
                _submitReq.setCmdPrefix(_app.getJobAttributes().getCmdPrefix());
        
        // Canonicalize by replacing empty strings with null.
        _submitReq.setMpiCmd(StringUtils.stripToNull(_submitReq.getMpiCmd()));
        _submitReq.setCmdPrefix(StringUtils.stripToNull(_submitReq.getCmdPrefix()));
        
        // Validate when MPI is indicated.
        if (_submitReq.getIsMpi()) {
            if (_submitReq.getMpiCmd() == null) {
                String msg = MsgUtils.getMsg("JOBS_MISSING_MPI_CMD");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            if (_submitReq.getCmdPrefix() != null) {
                String msg = MsgUtils.getMsg("JOBS_MPI_CMD_CONFLICT");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* finalizePaths:                                                               */
    /* ---------------------------------------------------------------------------- */
    /** This method does the final checking and normalizing of paths.  The path 
     * sanitizer is called a second time in case ".." was introduced via macro
     * substitution.  We then contract multiple slashes and remove trailing slashes.  
     * 
     * @throws TapisImplException
     */
    private void finalizePaths() throws TapisImplException
    {
        // Detect ".." segments in macro-expanded paths.
        sanitizeDirectoryPathnames();
        
        // Canonicalize path.
        canonicalizeDirectoryPathnames();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* sanitizeDirectoryPathnames:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Check the assigned directory pathnames for prohibited path traversal characters.
     * 
     * @throws TapisImplException
     */
    private void sanitizeDirectoryPathnames() throws TapisImplException
    {
        // Check each of the user specified directories.
        sanitizePath(_submitReq.getExecSystemInputDir(),  "ExecSystemInputDir");
        sanitizePath(_submitReq.getExecSystemExecDir(),   "ExecSystemExecDir");
        sanitizePath(_submitReq.getExecSystemOutputDir(), "ExecSystemOutputDir");
        sanitizePath(_submitReq.getArchiveSystemDir(),    "ArchiveSystemDir");
    }
    
    /* ---------------------------------------------------------------------------- */
    /* sanitizePath:                                                                */
    /* ---------------------------------------------------------------------------- */
    private void sanitizePath(String path, String displayName) throws TapisImplException
    {
        if (PathSanitizer.hasParentTraversal(path)) {
            String msg = MsgUtils.getMsg("TAPIS_PROHIBITED_DIR_PATTERN", displayName, path);
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* canonicalizeDirectoryPathnames:                                              */
    /* ---------------------------------------------------------------------------- */
    /** Replace multiple slashes with a single slash and remove trailing slashes.
     * This method is called after all macro substitution.
     * 
     * This method is intended to work on posix path names only (not uri's).
     * 
     * @throws TapisImplException if canonicalization fails
     */
    private void canonicalizeDirectoryPathnames() throws TapisImplException
    {
        // --------------------- Canonicalize ExecSystemInputDir -----------------
        try {_submitReq.setExecSystemInputDir(Path.of(_submitReq.getExecSystemInputDir()).toString());}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CANONICALIZE_PATH_ERROR",
                                             "ExecSystemInputDir", _submitReq.getExecSystemInputDir());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        // --------------------- Canonicalize ExecSystemExecDir ------------------
        try {_submitReq.setExecSystemExecDir(Path.of(_submitReq.getExecSystemExecDir()).toString());}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CANONICALIZE_PATH_ERROR",
                                             "ExecSystemExecDir", _submitReq.getExecSystemExecDir());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        // --------------------- Canonicalize ExecSystemOutputDir ----------------
        try {_submitReq.setExecSystemOutputDir(Path.of(_submitReq.getExecSystemOutputDir()).toString());}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CANONICALIZE_PATH_ERROR",
                                             "ExecSystemOutputDir", _submitReq.getExecSystemOutputDir());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        // --------------------- Canonicalize ArchiveSystemDir -------------------
        try {_submitReq.setArchiveSystemDir(Path.of(_submitReq.getArchiveSystemDir()).toString());}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CANONICALIZE_PATH_ERROR",
                                             "ArchiveSystemDir", _submitReq.getArchiveSystemDir());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeSubscriptions:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Add any subscriptions defined in the application to the request's
     * subscription list, converting from the app type to the request type as we go.
     * 
     * Request fields guaranteed to be assigned:
     *  - subscriptions
     * @throws TapisImplException on conversion failures
     */
    private void mergeSubscriptions() throws TapisImplException
    {
        // Add all app subscription requests to the job request's list.
        // Either or both may be empty. By the end of this method the
        // job request's subscription's list will be non-null.
        var subscriptions = _submitReq.getSubscriptions(); // force list creation
        if (_app.getJobAttributes().getSubscriptions() != null) 
            for (var appSub : _app.getJobAttributes().getSubscriptions()) {
                ReqSubscribe reqSub ;
                try {reqSub = new ReqSubscribe(appSub);}
                    catch (Exception e) {
                        throw new TapisImplException(e.getMessage(), 
                                                     Status.BAD_REQUEST.getStatusCode());
                    }
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
     */
    private void resolveFileInputs() throws TapisImplException
    {
        // Get the application's input file definitions.
        List<AppFileInput> appInputs = _app.getJobAttributes().getFileInputs();
        if (appInputs == null) appInputs = Collections.emptyList();
        var processedAppInputNames = new HashSet<String>(1 + appInputs.size() * 2);
        
        // Get the app's input strictness setting.
        boolean strictInputs;
        if (_app.getStrictFileInputs() == null)
            strictInputs = AppsClient.DEFAULT_STRICT_FILE_INPUTS;  
          else strictInputs = _app.getStrictFileInputs();
        
        // Process each request file input.
        var reqInputs = _submitReq.getFileInputs();  // forces list creation
        var it = reqInputs.listIterator();
        while (it.hasNext()) {
            // Current request input to process.
            var reqInput = it.next();
            
            // Process named and unnamed separately.
            if (StringUtils.isBlank(reqInput.getName())) {
                // ---------------- Unnamed Input ----------------
                // Are unnamed input files allowed by the application?
                if (strictInputs) {
                    String msg = MsgUtils.getMsg("JOBS_UNNAMED_FILE_INPUT", _app.getId(), reqInput.getSourceUrl());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Fill in any missing fields.
                completeRequestFileInput(reqInput);
            }
            else {
                // ----------------- Named Input -----------------
                // Get the app definition for this named file input.  Iterate 
                // through the list of definitions looking for a name match.
                String inputName = reqInput.getName();
                AppFileInput appInputDef = null;
                for (var def : appInputs) {
                    if (inputName.equals(def.getName())) {
                        appInputDef = def;
                        break;
                    }
                }
                
                // Make sure we found a matching definition when processing strictly.
                if (strictInputs && appInputDef == null) {
                    String msg = MsgUtils.getMsg("JOBS_NO_FILE_INPUT_DEFINITION", _app.getId(), inputName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Make sure this isn't a duplicate use of the same name.
                boolean added = processedAppInputNames.add(inputName);
                if (!added) {
                    String msg = MsgUtils.getMsg("JOBS_DUPLICATE_FILE_INPUT", _app.getId(), inputName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // When possible merge the application definition values into the request input. 
                if (appInputDef == null) completeRequestFileInput(reqInput);
                  else {
                      // Incomplete optional inputs are removed from the request.
                      var merged = mergeFileInput(reqInput, appInputDef);
                      if (!merged) it.remove();
                  }
            }
        }
        
        // By this point, all inputs specified in the request are accounted for and complete.
        // Incomplete OPTIONAL inputs that had a reqInput match have been removed, but those
        // not referenced in reqInput are still here and need to be skipped.
        // 
        // In general, we have to collect any complete REQUIRED and FIXED app inputs that were 
        // not referenced by the request input and add them to the request.  This will guarantee 
        // that all non-optional app inputs are included.
        for (var appInput : appInputs) {
            // Skip already merged inputs or, in the case of incomplete optional inputs,
            // already removed inputs.
            if (processedAppInputNames.contains(appInput.getName())) continue;
            
            // Skip incomplete optional inputs.
            if ((appInput.getInputMode() == null || appInput.getInputMode() == FileInputModeEnum.OPTIONAL) && 
                skipIncompleteFileInputs(appInput)) 
               continue;
            
            // Create a new request input from the REQUIRED or FIXED app input.
            var reqInput = JobFileInput.importAppInput(appInput);
            
            // Assign the shared app context flags.  The source flag is set only if the
            // tapis protocol is used; the destination is always on the execution system,
            // so we just check that the execution system input directory is shared.
            calculateSrcSharedCtx(reqInput, reqInput.getSourceUrl());
            reqInput.setDestSharedAppCtx(_sharedAppCtx.getSharingExecSystemInputDirAppOwner());
            
            // Canonicalize paths and derive other values.
            completeRequestFileInput(reqInput);
            
            // Add the input object to the request and record its name.
            // Recording the name will avoid processing duplicates, though
            // apps should disallow duplicates from the beginning.
            reqInputs.add(reqInput);
            processedAppInputNames.add(appInput.getName());
        }
        
        // Standardize the system name on tapislocal sources to be "exec.tapis" 
        // if the user specified some other systemId.  This replace is done on a best 
        // effort basis since the systemId is ignored when tapislocal is used.
        for (var reqInput : reqInputs) {
            String sourceUrl = reqInput.getSourceUrl();
            if (sourceUrl.startsWith(TapisLocalUrl.TAPISLOCAL_PROTOCOL_PREFIX) &&
                !sourceUrl.startsWith(TapisLocalUrl.TAPISLOCAL_FULL_PREFIX))
               try {reqInput.setSourceUrl(TapisLocalUrl.makeTapisLocalUrl(sourceUrl).toString());}
                    catch (Exception e) {}
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeFileInput:                                                              */
    /* ---------------------------------------------------------------------------- */
    /** Merge the application definition input values into the request input according
     * to established precedence rules.  This method is called when an input in 
     * the request has the same name as one in the application.  If the application
     * definition specifies a FIXED inputMode, then the request is not allowed to make
     * any changes to it.
     * 
     * The OPTIONAL inputMode means that it's ok for the input not to be staged for
     * any reason.  Reasons include (1) not having a complete definition as statically 
     * determined here, and (2) the source does not exist as dynamically determined at
     * runtime by the Files service.
     * 
     * @param reqInput non-null request input
     * @param appDef non-null application definition input
     * @return true if merge succeeded, false if optional input should be discarded
     * @throws TapisImplException
     */
    private boolean mergeFileInput(JobFileInput reqInput, AppFileInput appDef)
     throws TapisImplException
    {
        // Get the input mode to enforce merge semantics.  Incomplete OPTIONAL
        // inputs do not cause an error, but return false so they can be ignored. 
        final var inputMode = appDef.getInputMode();
        
        // ---- FIXED Inputs
        // Assign app values when the inputMode is FIXED.
        if (inputMode == FileInputModeEnum.FIXED) {
            assignFixedFileInput(reqInput, appDef);
            return true;
        }
        
        // ---- REQUIRED or OPTIONAL Inputs
        // Assign the source if necessary.
        if (StringUtils.isBlank(reqInput.getSourceUrl()))
            reqInput.setSourceUrl(appDef.getSourceUrl());
        if (StringUtils.isBlank(reqInput.getSourceUrl())) {
            if (inputMode == FileInputModeEnum.OPTIONAL) return false; // ignore input
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId(), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        calculateSrcSharedCtx(reqInput, appDef.getSourceUrl());
        
        // Calculate the target if necessary.
        if (StringUtils.isBlank(reqInput.getTargetPath()))
            reqInput.setTargetPath(appDef.getTargetPath());
        if (StringUtils.isBlank(reqInput.getTargetPath()))
            reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
        if ("*".equals(reqInput.getTargetPath())) // assign default for asterisk
            reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
        if (StringUtils.isBlank(reqInput.getTargetPath())) {
            if (inputMode == FileInputModeEnum.OPTIONAL) return false; // ignore input
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), 
                                         reqInput.getSourceUrl(), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        reqInput.setDestSharedAppCtx(_sharedAppCtx.getSharingExecSystemInputDirAppOwner());
        
        // Fill in the automount flag.
        if (reqInput.getAutoMountLocal() == null)
            reqInput.setAutoMountLocal(appDef.getAutoMountLocal());
        if (reqInput.getAutoMountLocal() == null) 
            reqInput.setAutoMountLocal(AppsClient.DEFAULT_FILE_INPUT_AUTO_MOUNT_LOCAL);

        // Merge the descriptions if both exist.
        if (StringUtils.isBlank(reqInput.getDescription()))
            reqInput.setDescription(appDef.getDescription());
          else 
            reqInput.setDescription(
                appDef.getDescription() + "\n\n" + reqInput.getDescription());
        
        // Set the optional flag in the request input.
        if (inputMode == FileInputModeEnum.OPTIONAL) reqInput.setOptional(true);
        
        // Successfully merged into a complete request.
        return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignFixedFileInput:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** This method assigns the app definition's FIXED values to request fields if 
     * those fields are unassigned.  If the request fields are already assigned,
     * then they must have the exact same value as in the app definition.
     * 
     * This method can only be called if the inputMode is FIXED.
     * 
     * @param reqInput request input
     * @param appDef app definition input
     * @throws TapisImplException on attempt to override a FIXED field value
     */
    private void assignFixedFileInput(JobFileInput reqInput, AppFileInput appDef) 
     throws TapisImplException
    {
        // Assign app values unless request field already has same value.
        
        // ---- sourceUrl
        if (StringUtils.isBlank(reqInput.getSourceUrl()))
            reqInput.setSourceUrl(appDef.getSourceUrl());
        else if (!reqInput.getSourceUrl().equals(appDef.getSourceUrl())) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "sourceUrl", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        // The app definition should not allow this, but we doublecheck.
        if (StringUtils.isBlank(reqInput.getSourceUrl())) {
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId(), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        calculateSrcSharedCtx(reqInput, appDef.getSourceUrl());
        
        // ---- targetPath
        if (StringUtils.isBlank(reqInput.getTargetPath()))
            reqInput.setTargetPath(appDef.getTargetPath());
        else if (!reqInput.getTargetPath().equals(appDef.getTargetPath())) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "targetPath", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        if ("*".equals(reqInput.getTargetPath())) // assign default for asterisk
            reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
        // The app definition should not allow this, but we doublecheck.
        if (StringUtils.isBlank(reqInput.getTargetPath())) {
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), 
                                         reqInput.getSourceUrl(), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        reqInput.setDestSharedAppCtx(_sharedAppCtx.getSharingExecSystemInputDirAppOwner());
        
        // ---- description
        if (StringUtils.isBlank(reqInput.getDescription()))
            reqInput.setDescription(appDef.getDescription());
        else if (!reqInput.getDescription().equals(appDef.getDescription())) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "destination", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // ---- autoMountLocal
        if (reqInput.getAutoMountLocal() == null)
            reqInput.setAutoMountLocal(appDef.getAutoMountLocal());
        else if (reqInput.getAutoMountLocal() != appDef.getAutoMountLocal()) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "autoMountLocal", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* completeRequestFileInput:                                                    */
    /* ---------------------------------------------------------------------------- */
    /** This method is called when a request file input does not match the name
     * of any file input specified in the application definition, such as when the
     * request input is anonymous or when a request input is created by importing an 
     * unreferenced app input.  In both cases, the sourceUrl must be set before 
     * calling this method.
     * 
     * This method is not appropriate for optional inputs that are allowed to be incomplete. 
     * 
     * @param reqInput a file input from the job request
     * @throws TapisImplException when source or target cannot be assigned
     */
    private void completeRequestFileInput(JobFileInput reqInput) throws TapisImplException
    {
        // Make sure we have a source path.
        if (StringUtils.isBlank(reqInput.getSourceUrl())) {
            var name = StringUtils.isBlank(reqInput.getName()) ? "unnamed" : reqInput.getName();
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId(), name);
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Set the target path if it's not set.
        if (StringUtils.isBlank(reqInput.getTargetPath()))
            reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
        if ("*".equals(reqInput.getTargetPath())) // assign default for asterisk
            reqInput.setTargetPath(TapisUtils.extractFilename(reqInput.getSourceUrl()));
        if (StringUtils.isBlank(reqInput.getTargetPath())) {
            var name = StringUtils.isBlank(reqInput.getName()) ? "unnamed" : reqInput.getName();
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), 
                                         reqInput.getSourceUrl(), name);
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        sanitizePath(reqInput.getTargetPath(), "fileInputs.targetPath");

        // Set the automount default value if needed.
        if (reqInput.getAutoMountLocal() == null) 
            reqInput.setAutoMountLocal(AppsClient.DEFAULT_FILE_INPUT_AUTO_MOUNT_LOCAL);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* skipIncompleteFileInputs:                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Determine whether an app's fileInput definition is complete, used for
     * OPTIONAL input processing.
     * 
     * @param appInput file input from an app definition
     * @return true when input is incomplete and should be skipped, false otherwise.
     */
    private boolean skipIncompleteFileInputs(AppFileInput appInput)
    {
        // Skip incomplete inputs.
        if (StringUtils.isBlank(appInput.getSourceUrl()) ||
            StringUtils.isBlank(appInput.getTargetPath()))
            return true;
        
        // Continue processing the input.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* resolveFileInputArrays:                                                      */
    /* ---------------------------------------------------------------------------- */
    private void resolveFileInputArrays() throws TapisImplException
    {
        // Get the application's input file definitions.
        List<AppFileInputArray> appArrays = _app.getJobAttributes().getFileInputArrays();
        if (appArrays == null) appArrays = Collections.emptyList();
        var processedAppInputNames = new HashSet<String>(1 + appArrays.size() * 2);
        
        // Get the app's input strictness setting.
        boolean strictInputs;
        if (_app.getStrictFileInputs() == null)
            strictInputs = AppsClient.DEFAULT_STRICT_FILE_INPUTS;  
          else strictInputs = _app.getStrictFileInputs();
        
        // Process each request file input.
        var reqArrays = _submitReq.getFileInputArrays();  // forces list creation
        var it = reqArrays.listIterator();
        while (it.hasNext()) {
            // Current request input to process.
            var reqArray = it.next();
            
            // Process named and unnamed separately.
            if (StringUtils.isBlank(reqArray.getName())) {
                // ---------------- Unnamed Input ----------------
                // Are unnamed input files allowed by the application?
                if (strictInputs) {
                    var sources = reqArray.getSourceUrls();
                    var firstSource = sources == null ? null : sources.get(0);
                    String msg = MsgUtils.getMsg("JOBS_UNNAMED_FILE_INPUT", _app.getId(), firstSource);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Ignore and remove empty unnamed arrays.
                if (reqArray.emptySourceUrls()) {
                    it.remove();
                    continue;
                }
                
                // Fill in any missing fields.
                completeRequestFileInputArray(reqArray);
            }
            else {
                // ----------------- Named Input -----------------
                // Get the app definition for this named file input.  Iterate 
                // through the list of definitions looking for a name match.
                String inputName = reqArray.getName();
                AppFileInputArray appArray = null;
                for (var array : appArrays) 
                    if (inputName.equals(array.getName())) {
                        appArray = array;
                        break;
                    }
                
                // Make sure we found a matching definition when processing strictly.
                if (strictInputs && appArray == null) {
                    String msg = MsgUtils.getMsg("JOBS_NO_FILE_INPUT_DEFINITION", _app.getId(), inputName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Make sure this isn't a duplicate use of the same name.
                boolean added = processedAppInputNames.add(inputName);
                if (!added) {
                    String msg = MsgUtils.getMsg("JOBS_DUPLICATE_FILE_INPUT", _app.getId(), inputName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // When possible merge the application definition values into the request input. 
                if (appArray == null) completeRequestFileInputArray(reqArray);
                  else {
                      // Incomplete optional inputs are removed from the request.
                      var merged = mergeFileInputArray(reqArray, appArray);
                      if (!merged) it.remove();
                  }
            }
        }
        
        // By this point, all inputs specified in the request are accounted for and complete.
        // Incomplete OPTIONAL inputs that had a reqArray match have been removed, but those
        // not referenced in reqArray are still here and need to be skipped.
        // 
        // In general, we have to collect any complete REQUIRED and FIXED app inputs that were 
        // not referenced by the request input and add them to the request.  This will guarantee 
        // that all non-optional app inputs are included.
        for (var appArray : appArrays) {
            // Skip already merged inputs or, in the case of incomplete optional inputs,
            // already removed inputs.
            if (processedAppInputNames.contains(appArray.getName())) continue;
            
            // Skip incomplete optional input arrays.
            if ((appArray.getInputMode() == null || appArray.getInputMode() == FileInputModeEnum.OPTIONAL) && 
                skipIncompleteFileInputArrays(appArray)) 
               continue;
            
            // Create a new request input from the REQUIRED or FIXED app input.
            var reqArray = JobFileInputArray.importAppInputArray(appArray);
            completeRequestFileInputArray(reqArray);
            
            // Always assign the shared app context flags since there is no possibility
            // that the app definition can be overridden.  The final determination of
            // the shared context setting for each source file is handled by the 
            // marshaling method below.
            if (_sharedAppCtx.isSharingEnabled()) reqArray.setSrcSharedAppCtx(_sharedAppCtx.getSharedAppOwner());
            reqArray.setDestSharedAppCtx(_sharedAppCtx.getSharingExecSystemInputDirAppOwner());
            
            // Add the request to the list and update list of processed names.
            // We rely on Apps to not allow duplicate named input arrays.
            reqArrays.add(reqArray);
            processedAppInputNames.add(appArray.getName());
        }
        
        // Add the array inputs to the JobFileInput list, which is the only list saved
        // in the job record.  This method also calculates the path name of each target
        // file/directory based on each source url and the shared ctx flags.
        if (!reqArrays.isEmpty()) marshallFileInputArrays(reqArrays);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeFileInputArray:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Merge the application definition input values into the request input according
     * to established precedence rules.  This method is called when an input array in
     * the request has the same name as one in the application.  If the application
     * definition specifies a FIXED inputMode, then the request is not allowed to make
     * any changes to it. 
     * 
     * The OPTIONAL inputMode means that it's ok for the input not to be staged for
     * any reason.  Reasons include (1) not having a complete definition as statically 
     * determined here, and (2) the source does not exist as dynamically determined at
     * runtime by the Files service.
     * 
     * @param reqInput non-null request input array
     * @param appDef non-null application definition input array
     * @return true if merge succeeded, false if optional input should be discarded
     * @throws TapisImplException
     */
    private boolean mergeFileInputArray(JobFileInputArray reqInput, AppFileInputArray appDef)
     throws TapisImplException
    {
        // Get the input mode to enforce merge semantics.  Incomplete OPTIONAL
        // inputs do not cause an error, but return false so they can be ignored. 
        final var inputMode = appDef.getInputMode();
        
        // ---- FIXED Inputs
        // Assign app values when the inputMode is FIXED.
        if (inputMode == FileInputModeEnum.FIXED) {
            assignFixedFileInputArray(reqInput, appDef);
            return true;
        }
        
        // ---- REQUIRED or OPTIONAL Inputs
        // Assign the source if necessary.
        if (reqInput.emptySourceUrls())
            reqInput.setSourceUrls(appDef.getSourceUrls());
        if (reqInput.emptySourceUrls()) {
            if (inputMode == FileInputModeEnum.OPTIONAL) return false; // ignore input
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId(), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        calculateSrcSharedCtxArray(reqInput, appDef.getSourceUrls());
        
        // Calculate the target if necessary.
        if (StringUtils.isBlank(reqInput.getTargetDir()))
            reqInput.setTargetDir(appDef.getTargetDir());
        if (StringUtils.isBlank(reqInput.getTargetDir()))
            reqInput.setTargetDir("/"); // indicates execSystemInputDir
        if ("*".equals(reqInput.getTargetDir())) // assign default for asterisk
            reqInput.setTargetDir("/"); // indicates execSystemInputDir
        if (StringUtils.isBlank(reqInput.getTargetDir())) {
            if (inputMode == FileInputModeEnum.OPTIONAL) return false; // ignore input
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), 
                                         reqInput.getSourceUrls().get(0), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        reqInput.setDestSharedAppCtx(_sharedAppCtx.getSharingExecSystemInputDirAppOwner());
        
        // Merge the descriptions if both exist.
        if (StringUtils.isBlank(reqInput.getDescription()))
            reqInput.setDescription(appDef.getDescription());
          else 
            reqInput.setDescription(
                appDef.getDescription() + "\n\n" + reqInput.getDescription());
        
        // Set the optional flag in the request input.
        if (inputMode == FileInputModeEnum.OPTIONAL) reqInput.setOptional(true);
        
        // Successfully merged into a complete request.
        return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignFixedFileInput:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** This method assigns the app definition's FIXED values to request fields if 
     * those fields are unassigned.  If the request fields are already assigned,
     * then they must have the exact same value as in the app definition.
     * 
     * This method can only be called if the inputMode is FIXED. 
     * 
     * @param reqInput request input array
     * @param appDef app definition input array
     * @throws TapisImplException on attempt to override a FIXED field value
     */
    private void assignFixedFileInputArray(JobFileInputArray reqInput, AppFileInputArray appDef) 
     throws TapisImplException
    {
        // Assign app values unless request field already has same value.
        
        // ---- sourceUrls
        if (reqInput.emptySourceUrls())
            reqInput.setSourceUrls(appDef.getSourceUrls());
        else if (!reqInput.equalSourceUrls​(appDef.getSourceUrls())) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "sourceUrls", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        // The app definition should not allow this, but we doublecheck.
        if (reqInput.emptySourceUrls()) {
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId(), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        calculateSrcSharedCtxArray(reqInput, appDef.getSourceUrls());
        
        // ---- targetDir
        if (StringUtils.isBlank(reqInput.getTargetDir()))
            reqInput.setTargetDir(appDef.getTargetDir());
        else if (!reqInput.getTargetDir().equals(appDef.getTargetDir())) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "targetDir", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Designate the execSystemInputDir as the default when asterisk is used.
        if ("*".equals(reqInput.getTargetDir())) reqInput.setTargetDir("/");
        // The app definition should not allow this, but we doublecheck.
        if (StringUtils.isBlank(reqInput.getTargetDir())) {
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), 
                                         reqInput.getSourceUrls().get(0), reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        reqInput.setDestSharedAppCtx(_sharedAppCtx.getSharingExecSystemInputDirAppOwner());
        
        // ---- description
        if (StringUtils.isBlank(reqInput.getDescription()))
            reqInput.setDescription(appDef.getDescription());
        else if (!reqInput.getDescription().equals(appDef.getDescription())) {
            String msg = MsgUtils.getMsg("JOBS_FIXED_INPUT_ERROR", _app.getId(), 
                                         "destination", reqInput.getName());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* completeRequestFileInputArray:                                               */
    /* ---------------------------------------------------------------------------- */
    /** This method is called when a request file input array does not match the name
     * of any file input specified in the application definition, such as when the the
     * request input is anonymous or when when a request input is created by importing 
     * an unreferenced app input array.  In both cases, the sourceUrls must be set before 
     * calling this method.
     * 
     * This method is not appropriate for optional input arrays that are allowed to be incomplete. 
     * 
     * @param reqInput a file input array from the job request
     * @throws TapisImplException when source or target cannot be assigned
     */
    private void completeRequestFileInputArray(JobFileInputArray reqInput) throws TapisImplException
    {
        // Make sure we have at least one element in the source url list.
        if (reqInput.emptySourceUrls()) {
            var name = StringUtils.isBlank(reqInput.getName()) ? "unnamed" : reqInput.getName(); 
            String msg = MsgUtils.getMsg("JOBS_NO_SOURCE_URL", _app.getId(), name);
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Set the target directory if it's not set.  The default is to designate
        // the execSystemInputDir as the target directory.  An asterisk is an
        // alternate way of specifying the execSystemInputDir.
        if (StringUtils.isBlank(reqInput.getTargetDir())) reqInput.setTargetDir("/");
        if ("*".equals(reqInput.getTargetDir())) reqInput.setTargetDir("/");
        if (StringUtils.isBlank(reqInput.getTargetDir())) {
            var name = StringUtils.isBlank(reqInput.getName()) ? "unnamed" : reqInput.getName();
            String msg = MsgUtils.getMsg("JOBS_NO_TARGET_PATH", _app.getId(), 
                                         reqInput.getSourceUrls().get(0), name);
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        sanitizePath(reqInput.getTargetDir(), "fileInputArrays.targetDir");
    }
    
    /* ---------------------------------------------------------------------------- */
    /* skipIncompleteFileInputArrays:                                               */
    /* ---------------------------------------------------------------------------- */
    /** Determine whether an app's fileInputArray definition is complete, used for
     * OPTIONAL input processing.
     * 
     * @param appArray file input from an app definition
     * @return true when input is incomplete and should be skipped, false otherwise.
     */
    private boolean skipIncompleteFileInputArrays(AppFileInputArray appArray)
    {
        // Skip incomplete inputs.
        if (appArray.getSourceUrls() == null || appArray.getSourceUrls().isEmpty() ||
            StringUtils.isBlank(appArray.getTargetDir()))
            return true;
        
        // Continue processing the input.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* marshallFileInputArrays:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Each of the sourceUrls in each of the input arrays is marshalled into a 
     * JobFileInput object and added to the submission request's JobFileInput list.
     * 
     * The goal is for the generated JobFileInput objects so contain as little 
     * redundancy as possible while maintaining traceability back to their original
     * user-specified array.
     * 
     * Final assignment of shared application context settings is performed here
     * for inputs specified in arrays.
     * 
     * @param arrays non-null list of input array objects
     */
    private void marshallFileInputArrays(List<JobFileInputArray> arrays)
     throws TapisImplException
    {
        // Process each array.
        for (int i = 0; i < arrays.size(); i++) {
            // Get the current array and check for work.
            var curArray = arrays.get(i);
            if (curArray.emptySourceUrls()) continue;
            
            // Calculate the full name for the 1st item.  Handle 3 cases:
            //  - unnamed array
            //  - name + suffix is too big
            //  - name + suffix is not too big
            String name1;
            final String suffix = "_" + (i+1) + ".";
            if (StringUtils.isBlank(curArray.getName())) name1 = suffix + "1";
            else if (curArray.getName().length() + suffix.length() > MAX_INPUT_NAME_LEN) {
                int subtrahend = curArray.getName().length() + suffix.length() + 1 - MAX_INPUT_NAME_LEN;
                name1 = curArray.getName().substring(0, curArray.getName().length() - subtrahend) + 
                        suffix + "1"; // example: 1st item in 1st array ends with "_1.1"
            }
            else name1 = curArray.getName() + suffix + "1"; 
            
            // Iterate through the source urls.
            for (int j = 0; j < curArray.getSourceUrls().size(); j++) {
                // Create a new input object.
                var reqInput = new JobFileInput();
                
                // Populate a new input object.  The first array object gets full 
                // name and description information; the rest get an abbreviated name
                // and no description.  
                //
                // There is the possibility that users specify the same names that 
                // we generate, but that shouldn't be a problem because all name 
                // uniqueness checking has already been performed.
                reqInput.setName(j == 0 ? name1 : (suffix + (j+1)));
                reqInput.setDescription(j == 0 ? curArray.getDescription() : null);
                reqInput.setAutoMountLocal(Boolean.FALSE);
                reqInput.setOptional(curArray.isOptional());
                
                // Assign source path and prohibit tapislocal urls.
                reqInput.setSourceUrl(curArray.getSourceUrls().get(j));
                if (reqInput.getSourceUrl().startsWith(TapisLocalUrl.TAPISLOCAL_PROTOCOL_PREFIX)) {
                    String arrayName = StringUtils.isBlank(curArray.getName()) ? "unnamed" : curArray.getName();
                    String msg = MsgUtils.getMsg("JOBS_TAPISLOCAL_NOT_ALLOWED", 
                                                 reqInput.getSourceUrl(), arrayName);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
                
                // Assign target path.
                String target = Path.of(curArray.getTargetDir(), 
                                        TapisUtils.extractFilename(reqInput.getSourceUrl())).toString();
                reqInput.setTargetPath(target);
                
                // Set the shared context flags.
                if (!StringUtils.isBlank(curArray.isSrcSharedAppCtx()) &&
                    reqInput.getSourceUrl().startsWith(TapisUrl.TAPIS_PROTOCOL_PREFIX))
                    reqInput.setSrcSharedAppCtx(_sharedAppCtx.getSharedAppOwner());
                reqInput.setDestSharedAppCtx(curArray.isDestSharedAppCtx());
            
                // Save the new object in the 
                _submitReq.getFileInputs().add(reqInput);
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calculateSrcSharedCtx:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Set the source file input shared flag only if we are in a shared app context
     * and the request's effective source url string is the same as the one specified
     * in the application.  No action is taken unless both conditions are satisfied.  
     * 
     * The request source url must be non-null by the time this method is called.
     * 
     * @param reqInput input request with non-null sourceUrl
     * @param appSource input source url from app definition, could be null
     */
    private void calculateSrcSharedCtx(JobFileInput reqInput, String appSource)
    {
        // Are we in a shared app context?
        if (!_sharedAppCtx.isSharingEnabled()) return;
        
        // Only tapis urls involve systems and share checking.
        if (!reqInput.getSourceUrl().startsWith(TapisUrl.TAPIS_PROTOCOL_PREFIX))
            return;
        
        // Only set the shared flag if the app source is in effect.
        if (reqInput.getSourceUrl().equals(appSource)) 
            reqInput.setSrcSharedAppCtx(_sharedAppCtx.getSharedAppOwner());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calculateSrcSharedCtxArray:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Set the source file array shared flag only if we are in a shared app context
     * and the request's effective source url array is the same as the one specified
     * in the application.  No action is taken unless both conditions are satisfied.  
     * 
     * The request source url must be non-null by the time this method is called.
     * 
     * @param reqInput input request with non-null sourceUrl array
     * @param appSources input source urls from app definition, could be null
     */
    private void calculateSrcSharedCtxArray(JobFileInputArray reqArray, List<String> appSources)
    {
        // Are we in a shared app context?
        if (!_sharedAppCtx.isSharingEnabled()) return;

        // Only set the shared flag if the app and request sources exactly match.
        if (reqArray.equalSourceUrls​(appSources)) 
            reqArray.setSrcSharedAppCtx(_sharedAppCtx.getSharedAppOwner());
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
        _macros.put(JobTemplateVariables.EffectiveUserId.name(), _execSystem.getEffectiveUserId());
        
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
                // Use a resolver that targets the archive system, not the usually one that
                // that target's the exec system.
                var archiveMacroResolver = new MacroResolver(_archiveSystem, _macros);
                _submitReq.setArchiveSystemDir(archiveMacroResolver.resolve(_submitReq.getArchiveSystemDir()));
                _macros.put(JobTemplateVariables.ArchiveSystemDir.name(), _submitReq.getArchiveSystemDir());
            }
        } 
        catch (TapisException e) {
            throw new TapisImplException(e.getMessage(), e, Status.BAD_REQUEST.getStatusCode());
        }
        catch (Exception e) {
            throw new TapisImplException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        // Special case macro resolution.  The substitutions here happen after all 
        // the other substitutions have been resolved, so simple, non-recursive 
        // substitution is all that's needed.  _submitReq is directly updated.
        resolveSchedulerOptionMacros();
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
    /* resolveSchedulerOptionMacros:                                                */
    /* ---------------------------------------------------------------------------- */
    /** Resolve macros in certain distinguished fields if those fields are assigned
     * values.
     */
    private void resolveSchedulerOptionMacros()
    {
        // Get the parameter set.
        var parmset = _submitReq.getParameterSet();
        if (parmset == null) return;
        
        // Get the scheduler options.
        var schedOptions = parmset.getSchedulerOptions();
        if (schedOptions == null) return;
        
        // Iterate through the options looking for the 
        // ones that might contain macros.
        for (var argSpec : schedOptions) {
            var arg = argSpec.getArg();
            if (arg == null) continue; // shouldn't happen
            
            // We only perform macro substitution on specific arguments.
            // The substitution is simple and non-recursive because the 
            // replacement value does not itself contain macros.
            if (arg.startsWith("--job-name ") || arg.startsWith("-J "))
               argSpec.setArg(replaceMacros(arg));
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
                                             LoadSystemTypes systemType,
                                             String sharedAppCtx) 
      throws TapisImplException
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
    /** Validate that the logical queue name is defined in the execution system when
     * in BATCH mode.  If so, return the actual queue name on the hpc system associated
     * with tapis logical queue.
     * 
     * We should only get here if we are running in batch mode.
     * 
     * @param logicalQueueName name of the tapis queue defined in the exec system
     * @return null or the remote hpc queue name defined in logical queue
     * @throws TapisImplException
     */
    private String validateExecSystemLogicalQueue(String logicalQueueName) 
     throws TapisImplException
    {
        // We need a queue.
        if (StringUtils.isBlank(logicalQueueName)) {
            String msg = MsgUtils.getMsg("JOBS_NO_LOGICAL_QUEUE", _app.getId(), 
                                         _execSystem.getId(), _submitReq.getTenant());
            throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
        
        // Validation will check that the named logical queue has been defined.
        for (var q : _execSystem.getBatchLogicalQueues()) 
            if (logicalQueueName.equals(q.getName())) return q.getHpcQueueName();

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
        if (!JobType.BATCH.name().equals(_submitReq.getJobType())) return;
        
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
    /* validateSchedulerProfile:                                                    */
    /* ---------------------------------------------------------------------------- */
    /** This method validates the existence of a scheduler profile if one is specified
     * with the --tapis-profile argument.  If the argument is not specified or if it is
     * specified and the profile exists, this method simply returns.  If the argument
     * is specified and the profile cannot be retrieved for any reason, this method
     * throws an expection.
     * 
     * @param schedulerOptions a list of scheduler options
     * @throws TapisImplException if a specified scheduler profile is inaccessible
     */
    private void validateSchedulerProfile(List<JobArgSpec> schedulerOptions) 
     throws TapisImplException
    {
        // Guard.
        if (schedulerOptions == null || schedulerOptions.isEmpty()) return;
        
        // Argument search spec.
        final String searchSpec = Job.TAPIS_PROFILE_KEY;
        
        // See if a profile is specified.
        for (var opt : schedulerOptions) {
            String arg = opt.getArg().strip();
            if (!arg.startsWith(searchSpec)) continue;
            if (arg.equals(searchSpec)) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_PROFILE_NO_NAME",
                                             _submitReq.getOwner(), _submitReq.getTenant());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            // Skip other arguments that start with the same prefix.
            if (arg.charAt(searchSpec.length()) != ' ') continue;
            
            // Get the profile name that is expected to follow 
            // the argument specifier.
            String profileName = arg.substring(searchSpec.length()+1).strip();
            
            // The profile exists if we can retrieve it.
            var client = getSystemsClient();
            SchedulerProfile profile = null;
            try {profile = client.getSchedulerProfile(profileName);}
                catch (Exception e) {
                    // Not found error.
                    if ((e instanceof TapisClientException) && 
                        ((TapisClientException)e).getCode() == 404) 
                    { 
                        String msg = MsgUtils.getMsg("JOBS_SCHEDULER_PROFILE_NOT_FOUND",
                                _submitReq.getOwner(), _submitReq.getTenant(), profileName);
                        throw new TapisImplException(msg, Status.NOT_FOUND.getStatusCode());
                    }
                    
                    // All other error cases.
                    String msg = MsgUtils.getMsg("JOBS_SCHEDULER_PROFILE_ACCESS_ERROR",
                                _submitReq.getOwner(), _submitReq.getTenant(), profileName, e.getMessage());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            _log.info(MsgUtils.getMsg("JOBS_SCHEDULER_PROFILE_FOUND", profileName, _submitReq.getTenant()));
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateNotes:                                                               */
    /* ---------------------------------------------------------------------------- */
    private void validateNotes() throws TapisImplException
    {
        // See if there are any notes in the request or, if not, the app.
        Object notes = _submitReq.getNotes();
        if (notes == null) notes = _app.getNotes();
        
        // This utility method can throw exceptions with correctly assigned response codes.
        _submitReq.setNotesAsString(JobsApiUtils.convertInputObjectToString(notes));
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
        
        // The conversion was already tested, so no risk of a runtime exception.
        _job.setJobType(JobType.valueOf(_submitReq.getJobType()));
        
        // Creator fields already validated.
        _job.setCreatedby(_threadContext.getOboUser());
        _job.setCreatedbyTenant(_threadContext.getOboTenantId());
        
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
        
        // MPI and command prefix.
        _job.setMpi(_submitReq.getIsMpi());
        _job.setMpiCmd(_submitReq.getMpiCmd());
        _job.setCmdPrefix(_submitReq.getCmdPrefix());
        
        // Set the shared context information.
        if (_sharedAppCtx.isSharingEnabled()) {
            _job.setSharedAppCtx(_sharedAppCtx.getSharedAppOwner());
            _job.setSharedAppCtxAttribs(_sharedAppCtx.getSharedAppCtxResources());
        }
        
        // Notes always has a well-formed json string representation of a json object.
        _job.setNotes(_submitReq.getNotesAsString()); 
        
        // Assign tapisQueue now that the job object is completely initialized.
        _job.setTapisQueue(new SelectQueueName().select(_job));
        
        // Set the hpc queue name which is only non-null on batch jobs.
        _job.setRemoteQueue(_submitReq.getHpcQueueName());
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

package edu.utexas.tacc.tapis.jobs.model;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.model.submit.JobParameterSet;
import edu.utexas.tacc.tapis.jobs.model.submit.JobSharedAppCtx.JobSharedAppCtxEnum;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;
import io.swagger.v3.oas.annotations.media.Schema;

public final class Job
{
	// Constants.
	public static final int DEFAULT_NODE_COUNT = 1;
	public static final int DEFAULT_CORES_PER_NODE = 1;
	public static final int DEFAULT_MEM_MB = 100;
	public static final int DEFAULT_MAX_MINUTES = 10;
	public static final int MAX_LAST_MESSAGE_LEN = 16384;
	public static final Boolean DEFAULT_ARCHIVE_ON_APP_ERROR = Boolean.TRUE;
	public static final Boolean DEFAULT_DYNAMIC_EXEC_SYSTEM = Boolean.FALSE;
	public static final String EMPTY_JSON = "{}";
	public static final String EMPTY_JSON_ARRAY = "[]";
	
	// Default directory assignments.  All paths are relative to their system's 
	// rootDir unless otherwise noted.  Leading slashes are optional on relative
	// paths and required on absolute paths.  When the full path names of relative 
	// paths are constructed, double slashes at the point of concatenation are prevented.
	public static final String DEFAULT_EXEC_SYSTEM_INPUT_DIR   = "/${JobWorkingDir}/jobs/${JobUUID}";
	public static final String DEFAULT_EXEC_SYSTEM_EXEC_DIR    = DEFAULT_EXEC_SYSTEM_INPUT_DIR;
	public static final String DEFAULT_EXEC_SYSTEM_OUTPUT_DIR  = DEFAULT_EXEC_SYSTEM_INPUT_DIR + "/output";
	public static final String DEFAULT_DTN_SYSTEM_INPUT_DIR    = "/${DtnMountPoint}/jobs/${JobUUID}";
	public static final String DEFAULT_DTN_SYSTEM_EXEC_DIR     = DEFAULT_DTN_SYSTEM_INPUT_DIR;
	public static final String DEFAULT_DTN_SYSTEM_OUTPUT_DIR   = DEFAULT_DTN_SYSTEM_INPUT_DIR + "/output";
    public static final String DEFAULT_ARCHIVE_SYSTEM_DIR      = "/jobs/${JobUUID}/archive";
    public static final String DEFAULT_DTN_SYSTEM_ARCHIVE_DIR  = DEFAULT_DTN_SYSTEM_INPUT_DIR + "/archive";
    
    // Standard container mountpoints.
    public static final String DEFAULT_EXEC_SYSTEM_INPUT_MOUNTPOINT  = "/TapisInput";
    public static final String DEFAULT_EXEC_SYSTEM_OUTPUT_MOUNTPOINT = "/TapisOutput";
    public static final String DEFAULT_EXEC_SYSTEM_EXEC_MOUNTPOINT   = "/TapisExec";
	
    // Prefix for reserved template variables (macros).
    public static final String TAPIS_ENV_VAR_PREFIX = "_tapis";
    
    // Tapis-specific scheduler option.
    public static final String TAPIS_PROFILE_KEY = "--tapis-profile";
	
	// Fields
    private int      			id;
    private String   			name;
    private String   			owner;
    private String   			tenant;
    private String   			description;
    
    private JobStatusType   	status = JobStatusType.PENDING;
    
    private String   			lastMessage;
    private Instant  			created;
    private Instant  			ended;
    private Instant  			lastUpdated;
    
    private String   			uuid;
    private String   			appId;
    private String   			appVersion;
    
    private boolean  			archiveOnAppError = DEFAULT_ARCHIVE_ON_APP_ERROR;
    private boolean             dynamicExecSystem = DEFAULT_DYNAMIC_EXEC_SYSTEM;
    
    private String   			execSystemId;
    private String   			execSystemExecDir;
    private String   			execSystemInputDir;
    private String   			execSystemOutputDir;
    private String              execSystemLogicalQueue;
    
    private String   			archiveSystemId;
    private String   			archiveSystemDir;
    
    private String              dtnSystemId;
    private String              dtnMountSourcePath;
    private String              dtnMountPoint;
    
    private int      			nodeCount = DEFAULT_NODE_COUNT;
    private int      			coresPerNode = DEFAULT_CORES_PER_NODE;
    private int      			memoryMB = DEFAULT_MEM_MB;
    private int      			maxMinutes = DEFAULT_MAX_MINUTES;
    
    private String   			fileInputs = EMPTY_JSON_ARRAY;
    private String   			parameterSet = EMPTY_JSON;
    private String              execSystemConstraints;
    private String              subscriptions = EMPTY_JSON;
    
    private int      			blockedCount;
    private String   			remoteJobId;
    private String   			remoteJobId2;
    private JobRemoteOutcome   	remoteOutcome;
    private String   			remoteResultInfo;
    private String   			remoteQueue;
    private Instant  			remoteSubmitted;
    private Instant  			remoteStarted;
    private Instant  			remoteEnded;
    private int      			remoteSubmitRetries;
    private int      			remoteChecksSuccess;
    private int      			remoteChecksFailed;
    private Instant  			remoteLastStatusCheck;
    
    private String              inputTransactionId;
    private String              inputCorrelationId;
    private String              archiveTransactionId;
    private String              archiveCorrelationId;
    
    private String   			tapisQueue;
    private boolean  			visible = true;
    private String   			createdby;
    private String   			createdbyTenant;
    private TreeSet<String>     tags;
    
    private JobType             jobType; // should never be null after db migration
    private boolean             isMpi;
    private String              mpiCmd;
    private String              cmdPrefix;
    
    private String              sharedAppCtx;
    private List<JobSharedAppCtxEnum> sharedAppCtxAttribs;
    
    private String              notes = EMPTY_JSON; // Should never be null.
    
    // ------ Runtime-only fields that do not get saved in the database ------
    // -----------------------------------------------------------------------
    
    // Store a reference to the execution context as soon as the worker 
    // creates the context in TenantQueueProcessor.
    @Schema(hidden = true)
    private transient JobExecutionContext _jobCtx;
    
    // The parsed version of the fileInputs json string cached for future use. 
    @Schema(hidden = true)
    private List<JobFileInput>      _fileInputsSpec;
    
    // The parsed version of the parameterSet json string cached for future use. 
    @Schema(hidden = true)
    private JobParameterSet         _parameterSetModel;
    
    // Only one command at a time is stored, so there's the possibility
    // of an unread command being overwritten, but sending multiple
    // asynchronous commands to a job is indeterminate anyway. The field
    // contains the last unread asynchronous message sent to this job.
    @Schema(hidden = true)
    private final transient AtomicReference<CmdMsg> _cmdMsg = new AtomicReference<>(null);
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public Job()
    {
    	// This value only gets overwritten when populating from db.
    	setUuid(new TapisUUID(UUIDType.JOB).toString());
    	
    	// Set the initial time.
    	Instant now = Instant.now();
    	setCreated(now);
    	setLastUpdated(now);
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* toString:                                                                    */
    /* ---------------------------------------------------------------------------- */
    @Override
    public String toString() {return TapisUtils.toString(this);}

    /* ---------------------------------------------------------------------------- */
    /* getFileInputsSpec:                                                           */
    /* ---------------------------------------------------------------------------- */
    @Schema(hidden = true)
    public List<JobFileInput> getFileInputsSpec() 
    {
        // Cache a version of the input spec if it doesn't exist.
        if (_fileInputsSpec == null) {
            Type listType = new TypeToken<List<JobFileInput>>(){}.getType();
            _fileInputsSpec = TapisGsonUtils.getGson().fromJson(fileInputs, listType);
        }
        
        return _fileInputsSpec;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getParameterSetModel:                                                        */
    /* ---------------------------------------------------------------------------- */
    @Schema(hidden = true)
    public JobParameterSet getParameterSetModel() 
    {
        // Cache the parsed parameter set if it doesn't exist.
        if (_parameterSetModel == null)
            _parameterSetModel = TapisGsonUtils.getGson().fromJson(parameterSet, JobParameterSet.class);
        return _parameterSetModel;
    }

    /* ---------------------------------------------------------------------------- */
    /* isArchiveSameAsOutput:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Determine if the archive directory and the output directory are the same and
     * they are defined on the same system.  
     * 
     * @return true if the archive and output directories are same on the same system,
     *         false otherwise
     */
    @Schema(hidden = true)
    public boolean isArchiveSameAsOutput()
    {
        // Don't blow up if called before job is initialized.
        if (execSystemOutputDir == null ||
            archiveSystemDir    == null ||
            execSystemId        == null ||
            archiveSystemId     == null)
            return false;
        
        // Compare directories and systems.
        if (execSystemId.equals(archiveSystemId) &&
            execSystemOutputDir.equals(archiveSystemDir))
            return true;
        
        // Not the same.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isArchiveSameAsExec:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Determine if the archive directory and the exec directory are the same and
     * they are defined on the same system.  
     * 
     * @return true if the archive and exec directories are same on the same system,
     *         false otherwise
     */
    @Schema(hidden = true)
    public boolean isArchiveSameAsExec()
    {
        // Don't blow up if called before job is initialized.
        if (execSystemExecDir == null ||
            archiveSystemDir  == null ||
            execSystemId      == null ||
            archiveSystemId   == null)
            return false;
        
        // Compare directories and systems.
        if (execSystemId.equals(archiveSystemId) &&
            execSystemExecDir.equals(archiveSystemDir))
            return true;
        
        // Not the same.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getMpiOrCmdPrefixPadded:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Either one or zero of the mpiCmd and cmdPrefix are in effect at any time.  The
     * mpiCmd can only be in effect if isMpi is set.  Prior validation assures that 
     * mpiCmd and cmdPrefix cannot be in effect at the same time.  If neither are in
     * effect, the empty string is returned.  Null is never returned.
     * 
     * @return the empty string or either the mpiCmd or cmdPrefix with a trailing space
     */
    @Schema(hidden = true)
    public String getMpiOrCmdPrefixPadded()
    {
        if (isMpi) return mpiCmd + " ";
        if (!StringUtils.isBlank(cmdPrefix)) return cmdPrefix + " ";
        return "";
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateForExecution:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** Final validation performed just before job execution.  All required fields,
     * calculated or user supplied, have to be valid by this time.  All the requirements
     * of front-end processing and database constraints are double-checked here.
     * 
     * @throws JobException on invalid job content
     */
    @Schema(hidden = true)
    public void validateForExecution()
     throws JobException
    {
        // Check the expected values of all fields that should be assigned
        // after the job has been created in the database but before any 
        // execution processing has occurred.
        if (id < 1) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "id", id);
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(name)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "name");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(owner)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "owner");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "tenant");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(description)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "description");
            throw new JobException(msg);
        }
        if (status == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "status");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(lastMessage)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "lastMessage");
            throw new JobException(msg);
        }
        if (created == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "created");
            throw new JobException(msg);
        }
        if (lastUpdated == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "lastUpdated");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(uuid)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "uuid");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(appId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "appId");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(appVersion)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "appVersion");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(execSystemId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "execSystemId");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(execSystemExecDir)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "execSystemExecDir");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(execSystemInputDir)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "execSystemInputDir");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(execSystemOutputDir)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "execSystemOutputDir");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(archiveSystemId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "archiveSystemId");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(archiveSystemDir)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "archiveSystemDir");
            throw new JobException(msg);
        }
        if (!StringUtils.isBlank(dtnSystemId)) {
            if (StringUtils.isBlank(dtnMountPoint)) {
                String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "dtnMountPoint");
                throw new JobException(msg);
            }
            if (StringUtils.isBlank(dtnMountSourcePath)) {
                String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "dtnMountSourcePath");
                throw new JobException(msg);
            }
        }
        if (nodeCount < 1) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "nodeCount", nodeCount);
            throw new JobException(msg);
        }
        if (coresPerNode < 1) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "coresPerNode", coresPerNode);
            throw new JobException(msg);
        }
        if (memoryMB < 1) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "memoryMB", memoryMB);
            throw new JobException(msg);
        }
        if (maxMinutes < 1) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "maxMinutes", maxMinutes);
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(fileInputs)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "fileInputs");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(parameterSet)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "parameterSet");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(subscriptions)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "subscriptions");
            throw new JobException(msg);
        }
        if (remoteSubmitRetries < 0) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "remoteSubmitRetries", remoteSubmitRetries);
            throw new JobException(msg);
        }
        if (remoteChecksSuccess < 0) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "remoteChecksSuccess", remoteChecksSuccess);
            throw new JobException(msg);
        }
        if (remoteChecksFailed < 0) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "remoteChecksFailed", remoteChecksFailed);
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(tapisQueue)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "tapisQueue");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(createdby)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "createdby");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(createdbyTenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "createdbyTenant");
            throw new JobException(msg);
        }
        if (jobType == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "jobType");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(notes)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "notes");
            throw new JobException(msg);
        }
        
        // MPI and command prefix checks.
        if (isMpi) {
            // MPI command must be specified.
            if (StringUtils.isBlank(mpiCmd)) {
                String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateForExecution", "mpiCmd");
                throw new JobException(msg);
            }
            // No command prefix can be specified.
            if (StringUtils.isNotBlank(cmdPrefix)) {
                String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateForExecution", "cmdPrefix-conflict", cmdPrefix);
                throw new JobException(msg);
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* getTotalTasks:                                                         */
    /* ---------------------------------------------------------------------- */
    /** This method is only guaranteed to return valid information after the 
     * validateForExecution() tests pass.  This method calculates the total 
     * number of task as the product of coresPerNode and number of nodes.   
     * 
     * @return the total number of processors requested across all nodes
     */
    @Schema(hidden = true)
    public int getTotalTasks()
    {
        return coresPerNode * nodeCount;
    }
    
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public JobStatusType getStatus() {
		return status;
	}

	public void setStatus(JobStatusType status) {
		this.status = status;
	}

	public String getLastMessage() {
		return lastMessage;
	}

	public void setLastMessage(String lastMessage) {
		this.lastMessage = lastMessage;
	}

	@Schema(type = "string")
	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	@Schema(type = "string")
	public Instant getEnded() {
		return ended;
	}

	public void setEnded(Instant ended) {
		this.ended = ended;
	}

	@Schema(type = "string")
	public Instant getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Instant lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

	public boolean isArchiveOnAppError() {
		return archiveOnAppError;
	}

	public void setArchiveOnAppError(boolean archiveOnAppError) {
		this.archiveOnAppError = archiveOnAppError;
	}

	public boolean isDynamicExecSystem() {
		return dynamicExecSystem;
	}

	public void setDynamicExecSystem(boolean dynamicExecSystem) {
		this.dynamicExecSystem = dynamicExecSystem;
	}

	public String getExecSystemId() {
		return execSystemId;
	}

	public void setExecSystemId(String execSystemId) {
		this.execSystemId = execSystemId;
	}

	public String getExecSystemExecDir() {
		return execSystemExecDir;
	}

	public void setExecSystemExecDir(String execSystemExecDir) {
		this.execSystemExecDir = execSystemExecDir;
	}

	public String getExecSystemInputDir() {
		return execSystemInputDir;
	}

	public void setExecSystemInputDir(String execSystemInputDir) {
		this.execSystemInputDir = execSystemInputDir;
	}

	public String getExecSystemOutputDir() {
		return execSystemOutputDir;
	}

	public void setExecSystemOutputDir(String execSystemOutputDir) {
		this.execSystemOutputDir = execSystemOutputDir;
	}

    public String getExecSystemLogicalQueue() {
        return execSystemLogicalQueue;
    }

    public void setExecSystemLogicalQueue(String execSystemLogicalQueue) {
        this.execSystemLogicalQueue = execSystemLogicalQueue;
    }

	public String getArchiveSystemId() {
		return archiveSystemId;
	}

	public void setArchiveSystemId(String archiveSystemId) {
		this.archiveSystemId = archiveSystemId;
	}

	public String getArchiveSystemDir() {
		return archiveSystemDir;
	}

	public void setArchiveSystemDir(String archiveSystemDir) {
		this.archiveSystemDir = archiveSystemDir;
	}

    public String getDtnSystemId() {
        return dtnSystemId;
    }

    public void setDtnSystemId(String dtnSystemId) {
        this.dtnSystemId = dtnSystemId;
    }

    public String getDtnMountSourcePath() {
        return dtnMountSourcePath;
    }

    public void setDtnMountSourcePath(String dtnMountSourcePath) {
        this.dtnMountSourcePath = dtnMountSourcePath;
    }

    public String getDtnMountPoint() {
        return dtnMountPoint;
    }

    public void setDtnMountPoint(String dtnMountPoint) {
        this.dtnMountPoint = dtnMountPoint;
    }

	public int getNodeCount() {
		return nodeCount;
	}

	public void setNodeCount(int nodeCount) {
		this.nodeCount = nodeCount;
	}

	public int getCoresPerNode() {
		return coresPerNode;
	}

	public void setCoresPerNode(int coresPerNode) {
		this.coresPerNode = coresPerNode;
	}

	public int getMemoryMB() {
		return memoryMB;
	}

	public void setMemoryMB(int memoryMB) {
		this.memoryMB = memoryMB;
	}

	public int getMaxMinutes() {
		return maxMinutes;
	}

	public void setMaxMinutes(int maxMinutes) {
		this.maxMinutes = maxMinutes;
	}

	public String getFileInputs() {
		return fileInputs;
	}

	public void setFileInputs(String inputs) {
		this.fileInputs = inputs;
	}

	public String getParameterSet() {
		return parameterSet;
	}

	public void setParameterSet(String parameters) {
		this.parameterSet = parameters;
	}

	public String getExecSystemConstraints() {
		return execSystemConstraints;
	}

	public void setExecSystemConstraints(String execSystemConstraints) {
		this.execSystemConstraints = execSystemConstraints;
	}

	public String getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(String subscriptions) {
		this.subscriptions = subscriptions;
	}

	public int getBlockedCount() {
		return blockedCount;
	}

	public void setBlockedCount(int blockedCount) {
		this.blockedCount = blockedCount;
	}

	public String getRemoteJobId() {
		return remoteJobId;
	}

	public void setRemoteJobId(String remoteJobId) {
		this.remoteJobId = remoteJobId;
	}

	public String getRemoteJobId2() {
		return remoteJobId2;
	}

	public void setRemoteJobId2(String remoteJobId2) {
		this.remoteJobId2 = remoteJobId2;
	}

	public JobRemoteOutcome getRemoteOutcome() {
		return remoteOutcome;
	}

	public void setRemoteOutcome(JobRemoteOutcome remoteOutcome) {
		this.remoteOutcome = remoteOutcome;
	}

	public String getRemoteResultInfo() {
		return remoteResultInfo;
	}

	public void setRemoteResultInfo(String remoteResultInfo) {
		this.remoteResultInfo = remoteResultInfo;
	}

	public String getRemoteQueue() {
		return remoteQueue;
	}

	public void setRemoteQueue(String remoteQueue) {
		this.remoteQueue = remoteQueue;
	}

	@Schema(type = "string")
	public Instant getRemoteSubmitted() {
		return remoteSubmitted;
	}

	public void setRemoteSubmitted(Instant remoteSubmitted) {
		this.remoteSubmitted = remoteSubmitted;
	}

	@Schema(type = "string")
	public Instant getRemoteStarted() {
		return remoteStarted;
	}

	public void setRemoteStarted(Instant remoteStarted) {
		this.remoteStarted = remoteStarted;
	}

	@Schema(type = "string")
	public Instant getRemoteEnded() {
		return remoteEnded;
	}

	public void setRemoteEnded(Instant remoteEnded) {
		this.remoteEnded = remoteEnded;
	}

	public int getRemoteSubmitRetries() {
		return remoteSubmitRetries;
	}

	public void setRemoteSubmitRetries(int remoteSubmitRetries) {
		this.remoteSubmitRetries = remoteSubmitRetries;
	}

	public int getRemoteChecksSuccess() {
		return remoteChecksSuccess;
	}

	public void setRemoteChecksSuccess(int remoteChecksSuccess) {
		this.remoteChecksSuccess = remoteChecksSuccess;
	}

	public int getRemoteChecksFailed() {
		return remoteChecksFailed;
	}

	public void setRemoteChecksFailed(int remoteChecksFailed) {
		this.remoteChecksFailed = remoteChecksFailed;
	}

	@Schema(type = "string")
	public Instant getRemoteLastStatusCheck() {
		return remoteLastStatusCheck;
	}

	public void setRemoteLastStatusCheck(Instant remoteLastStatusCheck) {
		this.remoteLastStatusCheck = remoteLastStatusCheck;
	}

	public String getTapisQueue() {
		return tapisQueue;
	}

	public void setTapisQueue(String tapisQueue) {
		this.tapisQueue = tapisQueue;
	}

    public TreeSet<String> getTags() {
        return tags;
    }

    public void setTags(TreeSet<String> tags) {
        this.tags = tags;
    }
    
	public boolean isVisible() {
		return visible;
	}

	public void setVisible(boolean visible) {
		this.visible = visible;
	}

	public String getCreatedby() {
		return createdby;
	}

	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}

	public String getCreatedbyTenant() {
		return createdbyTenant;
	}

	public void setCreatedbyTenant(String createdbyTenant) {
		this.createdbyTenant = createdbyTenant;
	}

    public String getInputTransactionId() {
        return inputTransactionId;
    }

    public void setInputTransactionId(String inputTransactionId) {
        this.inputTransactionId = inputTransactionId;
    }

    public String getInputCorrelationId() {
        return inputCorrelationId;
    }

    public void setInputCorrelationId(String inputCorrelationId) {
        this.inputCorrelationId = inputCorrelationId;
    }

    public String getArchiveTransactionId() {
        return archiveTransactionId;
    }

    public void setArchiveTransactionId(String archiveTransactionId) {
        this.archiveTransactionId = archiveTransactionId;
    }

    public String getArchiveCorrelationId() {
        return archiveCorrelationId;
    }

    public void setArchiveCorrelationId(String archiveCorrelationId) {
        this.archiveCorrelationId = archiveCorrelationId;
    }

    public boolean isMpi() {
        return isMpi;
    }

    public void setMpi(boolean isMpi) {
        this.isMpi = isMpi;
    }

    public String getMpiCmd() {
        return mpiCmd;
    }

    public void setMpiCmd(String mpiCmd) {
        this.mpiCmd = mpiCmd;
    }

    public String getCmdPrefix() {
        return cmdPrefix;
    }

    public void setCmdPrefix(String cmdPrefix) {
        this.cmdPrefix = cmdPrefix;
    }

    public String getSharedAppCtx() {
        return sharedAppCtx;
    }

    public void setSharedAppCtx(String sharedAppCtx) {
        this.sharedAppCtx = sharedAppCtx;
    }

    public List<JobSharedAppCtxEnum> getSharedAppCtxAttribs() {
        return sharedAppCtxAttribs;
    }

    public void setSharedAppCtxAttribs(List<JobSharedAppCtxEnum> sharedAppCtxAttribs) {
        this.sharedAppCtxAttribs = sharedAppCtxAttribs;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        if (notes != null) this.notes = notes;
    }

    // Get the current cmdMsg value and atomically set the field to null.
    @Schema(hidden = true)
    public CmdMsg getAndSetCmdMsg() {
        return _cmdMsg.getAndSet(null);
    }
    
    // Get the current cmdMsg value and atomically set the field to a new value.
    @Schema(hidden = true)
    public CmdMsg getAndSetCmdMsg(CmdMsg cmdMsg) {
        return _cmdMsg.getAndSet(cmdMsg);
    }

    @Schema(hidden = true)
    public void setCmdMsg(CmdMsg cmdMsg) {
        _cmdMsg.set(cmdMsg);
    }

    @Schema(hidden = true)
    public JobExecutionContext getJobCtx() {
        return _jobCtx;
    }

    @Schema(hidden = true)
    public void setJobCtx(JobExecutionContext jobCtx) {
        this._jobCtx = jobCtx;
    }
}

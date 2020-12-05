package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import java.util.TreeSet;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobExecClass;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

public final class Job
{
	// Constants.
	public static final int DEFAULT_NODE_COUNT = 1;
	public static final int DEFAULT_CORES_PER_NODE = 1;
	public static final int DEFAULT_MEM_MB = 100;
	public static final int DEFAULT_MAX_MINUTES = 10;
	public static final Boolean DEFAULT_ARCHIVE_ON_APP_ERROR = Boolean.TRUE;
	public static final Boolean DEFAULT_USE_DTN = Boolean.TRUE;
	public static final Boolean DEFAULT_DYNAMIC_EXEC_SYSTEM = Boolean.FALSE;
	public static final String EMPTY_JSON = "{}";
	
	// Fields
    private int      			id;
    private String   			name;
    private String   			owner;
    private String   			tenant;
    private String   			description;
    private JobStatusType   	status = JobStatusType.PENDING;
    private JobType   			type = JobType.BATCH;
    private JobExecClass   		execClass = JobExecClass.NORMAL;
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
    private String   			archiveSystemId;
    private String   			archiveSystemDir;
    private String              dtnSystemId;
    private String              dtnMountPoint;
    private String              dtnSubDir;
    private int      			nodeCount = DEFAULT_NODE_COUNT;
    private int      			coresPerNode = DEFAULT_CORES_PER_NODE;
    private int      			memoryMB = DEFAULT_MEM_MB;
    private int      			maxMinutes = DEFAULT_MAX_MINUTES;
    private String   			fileInputs = EMPTY_JSON;
    private String   			parameterSet = EMPTY_JSON;
    private String              execSystemConstraints = EMPTY_JSON;
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
    private String   			tapisQueue;
    private TreeSet<String>     tags;
    private boolean  			visible;
    private String   			createdby;
    private String   			createdbyTenant;

    // Constructor
    public Job()
    {
    	// This value only gets overwritten when populating from db.
    	setUuid(new TapisUUID(UUIDType.JOB).toString());
    }
    
    @Override
    public String toString() {return TapisUtils.toString(this);}

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

	public JobType getType() {
		return type;
	}

	public void setType(JobType type) {
		this.type = type;
	}

	public JobExecClass getExecClass() {
		return execClass;
	}

	public void setExecClass(JobExecClass execClass) {
		this.execClass = execClass;
	}

	public String getLastMessage() {
		return lastMessage;
	}

	public void setLastMessage(String lastMessage) {
		this.lastMessage = lastMessage;
	}

	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	public Instant getEnded() {
		return ended;
	}

	public void setEnded(Instant ended) {
		this.ended = ended;
	}

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

    public String getDtnMountPoint() {
        return dtnMountPoint;
    }

    public void setDtnMountPoint(String dtnMountPoint) {
        this.dtnMountPoint = dtnMountPoint;
    }

    public String getDtnSubDir() {
        return dtnSubDir;
    }

    public void setDtnSubDir(String dtnSubDir) {
        this.dtnSubDir = dtnSubDir;
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

	public Instant getRemoteSubmitted() {
		return remoteSubmitted;
	}

	public void setRemoteSubmitted(Instant remoteSubmitted) {
		this.remoteSubmitted = remoteSubmitted;
	}

	public Instant getRemoteStarted() {
		return remoteStarted;
	}

	public void setRemoteStarted(Instant remoteStarted) {
		this.remoteStarted = remoteStarted;
	}

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
}

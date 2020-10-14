package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class Jobs
{
    private int     id;
    private String  name;
    private String  owner;
    private String  ownerTenant;
    private String  description;
    private String  status;
    private String  type;
    private String  execClass;
    private String  lastMessage;
    private Instant created;
    private Instant ended;
    private Instant lastUpdated;
    private String  uuid;
    private String  appId;
    private String  appVersion;
    private String  archiveOnAppError;
    private String  inputSystemId;
    private String  execSystemId;
    private String  execSystemExecPath;
    private String  execSystemInputPath;
    private String  archiveSystemId;
    private String  archiveSystemPath;
    private int     nodes;
    private int     processorsPerNode;
    private int     memoryMb;
    private int     maxMinutes;
    private String  inputs;
    private String  parameters;
    private String  events;
    private String  execSystemConstraints;
    private int     blockedCount;
    private String  remoteJobId;
    private String  remoteJobId2;
    private String  remoteOutcome;
    private String  remoteResultInfo;
    private String  remoteQueue;
    private Instant remoteSubmitted;
    private Instant remoteStarted;
    private Instant remoteEnded;
    private int     remoteSubmitRetries;
    private int     remoteChecksSuccess;
    private int     remoteChecksFailed;
    private Instant remoteLastStatusCheck;
    private String  tapisQueue;
    private String  visible;
    private String  createdby;
    private String  createdbyTenant;

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

	public String getOwnerTenant() {
		return ownerTenant;
	}

	public void setOwnerTenant(String ownerTenant) {
		this.ownerTenant = ownerTenant;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getExecClass() {
		return execClass;
	}

	public void setExecClass(String execClass) {
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

	public String getArchiveOnAppError() {
		return archiveOnAppError;
	}

	public void setArchiveOnAppError(String archiveOnAppError) {
		this.archiveOnAppError = archiveOnAppError;
	}

	public String getInputSystemId() {
		return inputSystemId;
	}

	public void setInputSystemId(String inputSystemId) {
		this.inputSystemId = inputSystemId;
	}

	public String getExecSystemId() {
		return execSystemId;
	}

	public void setExecSystemId(String execSystemId) {
		this.execSystemId = execSystemId;
	}

	public String getExecSystemExecPath() {
		return execSystemExecPath;
	}

	public void setExecSystemExecPath(String execSystemExecPath) {
		this.execSystemExecPath = execSystemExecPath;
	}

	public String getExecSystemInputPath() {
		return execSystemInputPath;
	}

	public void setExecSystemInputPath(String execSystemInputPath) {
		this.execSystemInputPath = execSystemInputPath;
	}

	public String getArchiveSystemId() {
		return archiveSystemId;
	}

	public void setArchiveSystemId(String archiveSystemId) {
		this.archiveSystemId = archiveSystemId;
	}

	public String getArchiveSystemPath() {
		return archiveSystemPath;
	}

	public void setArchiveSystemPath(String archiveSystemPath) {
		this.archiveSystemPath = archiveSystemPath;
	}

	public int getNodes() {
		return nodes;
	}

	public void setNodes(int nodes) {
		this.nodes = nodes;
	}

	public int getProcessorsPerNode() {
		return processorsPerNode;
	}

	public void setProcessorsPerNode(int processorsPerNode) {
		this.processorsPerNode = processorsPerNode;
	}

	public int getMemoryMb() {
		return memoryMb;
	}

	public void setMemoryMb(int memoryMb) {
		this.memoryMb = memoryMb;
	}

	public int getMaxMinutes() {
		return maxMinutes;
	}

	public void setMaxMinutes(int maxMinutes) {
		this.maxMinutes = maxMinutes;
	}

	public String getInputs() {
		return inputs;
	}

	public void setInputs(String inputs) {
		this.inputs = inputs;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getEvents() {
		return events;
	}

	public void setEvents(String events) {
		this.events = events;
	}

	public String getExecSystemConstraints() {
		return execSystemConstraints;
	}

	public void setExecSystemConstraints(String execSystemConstraints) {
		this.execSystemConstraints = execSystemConstraints;
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

	public String getRemoteOutcome() {
		return remoteOutcome;
	}

	public void setRemoteOutcome(String remoteOutcome) {
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

	public String getVisible() {
		return visible;
	}

	public void setVisible(String visible) {
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

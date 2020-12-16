package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.model.JobParameterSet;

public class ReqSubmitJob 
 implements IReqBody
{
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
    private String   			name;
    private String   			owner;
    private String   			tenant;
    private String   			description;
    private String   			appId;
    private String   			appVersion;
    private Boolean  			archiveOnAppError;  // not assigned by default
    private Boolean             dynamicExecSystem;  // not assigned by default
    private String   			execSystemId;
    private String   			execSystemExecDir;
    private String   			execSystemInputDir;
    private String   			execSystemOutputDir;
    private String   			archiveSystemId;
    private String   			archiveSystemDir;
    private Integer   			nodeCount;
    private Integer      		coresPerNode;
    private Integer      		memoryMB;
    private Integer      		maxMinutes;
    private String   			fileInputs;
    private JobParameterSet 	parameterSet;      // assigned on first use
    private String              execSystemConstraints = Job.EMPTY_JSON;
    private String              subscriptions = Job.EMPTY_JSON;
    private List<String>        tags;

	@Override
	public String validate() 
	{
		// Success.
		return null; // json schema validation is sufficient
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

	public Boolean isArchiveOnAppError() {
		return archiveOnAppError;
	}

	public void setArchiveOnAppError(Boolean archiveOnAppError) {
		this.archiveOnAppError = archiveOnAppError;
	}

	public Boolean isDynamicExecSystem() {
		return dynamicExecSystem;
	}

	public void setDynamicExecSystem(Boolean dynamicExecSystem) {
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

	public Integer getNodeCount() {
		return nodeCount;
	}

	public void setNodeCount(Integer nodeCount) {
		this.nodeCount = nodeCount;
	}

	public Integer getCoresPerNode() {
		return coresPerNode;
	}

	public void setCoresPerNode(Integer coresPerNode) {
		this.coresPerNode = coresPerNode;
	}

	public Integer getMemoryMB() {
		return memoryMB;
	}

	public void setMemoryMB(Integer memoryMB) {
		this.memoryMB = memoryMB;
	}

	public Integer getMaxMinutes() {
		return maxMinutes;
	}

	public void setMaxMinutes(Integer maxMinutes) {
		this.maxMinutes = maxMinutes;
	}

	public String getFileInputs() {
		return fileInputs;
	}

	public void setFileInputs(String inputs) {
		this.fileInputs = inputs;
	}

	public JobParameterSet getParameterSet() {
	    // Create parameter set on demand if needed.
	    if (parameterSet == null) parameterSet = new JobParameterSet();
		return parameterSet;
	}

	public void setParameterSet(JobParameterSet parameters) {
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

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}

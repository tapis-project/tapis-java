package edu.utexas.tacc.tapis.jobs.model.dto;

import java.time.Instant;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;



/** This is the Job Listing DTO class that constitutes some fields of a job required for job's listing response 
*
*/
public class JobListDTO {

	private String uuid;
	private String name;
	private String owner;

	private String appId;
	private Instant created;
	private JobStatusType status;
	private Instant remoteStarted; 
	private Instant ended;
	private String tenant;
	private String execSystemId;
	private String  archiveSystemId;
	
	private String  appVersion;

	private Instant  lastUpdated;
		
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
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
	public String getExecSystemId() {
		return execSystemId;
	}
	public void setExecSystemId(String execSystemId) {
		this.execSystemId = execSystemId;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public Instant getCreated() {
		return created;
	}
	public void setCreated(Instant created) {
		this.created = created;
	}
	public JobStatusType getStatus() {
		return status;
	}
	public void setStatus(JobStatusType status) {
		this.status = status;
	}
	public Instant getRemoteStarted() {
		return remoteStarted;
	}
	public void setRemoteStarted(Instant remoteStarted) {
		this.remoteStarted = remoteStarted;
	}
	public Instant getEnded() {
		return ended;
	}
	public void setEnded(Instant ended) {
		this.ended = ended;
	}
	public String getTenant() {
		return tenant;
	}
	public void setTenant(String tenant) {
		this.tenant = tenant;
	}
	public String getArchiveSystemId() {
		return archiveSystemId;
	}
	public void setArchiveSystemId(String archiveSystemId) {
		this.archiveSystemId = archiveSystemId;
	}
	public String getAppVersion() {
		return appVersion;
	}
	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}
	public Instant getLastUpdated() {
		return lastUpdated;
	}
	public void setLastUpdated(Instant lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
	public JobListDisplayDTO toJobListDisplayDTO(String tenantBaseURL) {
		return new JobListDisplayDTO(this,tenantBaseURL);
	}
}

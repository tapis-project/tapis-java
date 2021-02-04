package edu.utexas.tacc.tapis.jobs.model.dto;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;

public class JobStatusDTO {
	private String jobUuid;
	private JobStatusType status;
	private String owner;
	private int visible;
	private long jobId;
	private String tenant;
	private String createdby;
	
	public JobStatusDTO() {};

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}
	
	public JobStatusType getStatus() {
		return status;
	}
	public void setStatus(JobStatusType status) {
		this.status = status;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public int getVisible() {
		return visible;
	}
	public void setVisible(int visible) {
		this.visible = visible;
	}
	public long getJobId() {
		return jobId;
	}
	public void setJobId(long jobId) {
		this.jobId = jobId;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}
	public String getCreatedBy() {
		return createdby;
	}
	public void setCreatedBy(String createdby) {
		this.createdby = createdby;
	}
}

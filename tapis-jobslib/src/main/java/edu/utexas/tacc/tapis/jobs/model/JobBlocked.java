package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobBlocked
{
    private int     id;
    private int     recoveryId;
    private Instant created;
    private String  successStatus;
    private String  jobUuid;
    private String  statusMessage;

    @Override
    public String toString() {return TapisUtils.toString(this);}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getRecoveryId() {
		return recoveryId;
	}

	public void setRecoveryId(int recoveryId) {
		this.recoveryId = recoveryId;
	}

	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	public String getSuccessStatus() {
		return successStatus;
	}

	public void setSuccessStatus(String successStatus) {
		this.successStatus = successStatus;
	}

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	public void setStatusMessage(String statusMessage) {
		this.statusMessage = statusMessage;
	}
}

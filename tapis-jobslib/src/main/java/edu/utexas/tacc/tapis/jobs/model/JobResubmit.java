package edu.utexas.tacc.tapis.jobs.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobResubmit
{
    private int     id;
    private String  jobUuid;
    private String  jobDefinition;

    @Override
    public String toString() {return TapisUtils.toString(this);}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

	public String getJobDefinition() {
		return jobDefinition;
	}

	public void setJobDefinition(String jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
}

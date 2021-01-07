package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobEvent
{
    private int     id;
    private Instant created;
    private String  jobUuid;
    private String  othUuid;
    private String  event;
    private String  description;

    @Override
    public String toString() {return TapisUtils.toString(this);}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

	public String getOthUuid() {
		return othUuid;
	}

	public void setOthUuid(String othUuid) {
		this.othUuid = othUuid;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}

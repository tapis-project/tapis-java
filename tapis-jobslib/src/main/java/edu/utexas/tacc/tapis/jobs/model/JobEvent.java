package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobEvent
{
    private long          id;
    private JobEventType  event;
    private Instant       created;
    private String        jobUuid;
    private String        eventDetail;
    private String        othUuid;
    private String        description;

    @Override
    public String toString() {return TapisUtils.toString(this);}

	public long getId() {
		return id;
	}

	public void setId(long id) {
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

	public JobEventType getEvent() {
		return event;
	}

	public void setEvent(JobEventType event) {
		this.event = event;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

    public String getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(String eventDetail) {
        this.eventDetail = eventDetail;
    }
}

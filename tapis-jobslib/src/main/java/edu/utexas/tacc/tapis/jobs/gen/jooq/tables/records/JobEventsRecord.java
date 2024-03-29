/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records;


import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobEvents;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;

import java.time.LocalDateTime;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record8;
import org.jooq.Row8;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class JobEventsRecord extends UpdatableRecordImpl<JobEventsRecord> implements Record8<Long, JobEventType, LocalDateTime, String, String, String, String, String> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.job_events.id</code>.
     */
    public void setId(Long value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.job_events.id</code>.
     */
    public Long getId() {
        return (Long) get(0);
    }

    /**
     * Setter for <code>public.job_events.event</code>.
     */
    public void setEvent(JobEventType value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.job_events.event</code>.
     */
    public JobEventType getEvent() {
        return (JobEventType) get(1);
    }

    /**
     * Setter for <code>public.job_events.created</code>.
     */
    public void setCreated(LocalDateTime value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.job_events.created</code>.
     */
    public LocalDateTime getCreated() {
        return (LocalDateTime) get(2);
    }

    /**
     * Setter for <code>public.job_events.job_uuid</code>.
     */
    public void setJobUuid(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.job_events.job_uuid</code>.
     */
    public String getJobUuid() {
        return (String) get(3);
    }

    /**
     * Setter for <code>public.job_events.event_detail</code>.
     */
    public void setEventDetail(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.job_events.event_detail</code>.
     */
    public String getEventDetail() {
        return (String) get(4);
    }

    /**
     * Setter for <code>public.job_events.oth_uuid</code>.
     */
    public void setOthUuid(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.job_events.oth_uuid</code>.
     */
    public String getOthUuid() {
        return (String) get(5);
    }

    /**
     * Setter for <code>public.job_events.description</code>.
     */
    public void setDescription(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.job_events.description</code>.
     */
    public String getDescription() {
        return (String) get(6);
    }

    /**
     * Setter for <code>public.job_events.tenant</code>.
     */
    public void setTenant(String value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.job_events.tenant</code>.
     */
    public String getTenant() {
        return (String) get(7);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Long> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record8 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row8<Long, JobEventType, LocalDateTime, String, String, String, String, String> fieldsRow() {
        return (Row8) super.fieldsRow();
    }

    @Override
    public Row8<Long, JobEventType, LocalDateTime, String, String, String, String, String> valuesRow() {
        return (Row8) super.valuesRow();
    }

    @Override
    public Field<Long> field1() {
        return JobEvents.JOB_EVENTS.ID;
    }

    @Override
    public Field<JobEventType> field2() {
        return JobEvents.JOB_EVENTS.EVENT;
    }

    @Override
    public Field<LocalDateTime> field3() {
        return JobEvents.JOB_EVENTS.CREATED;
    }

    @Override
    public Field<String> field4() {
        return JobEvents.JOB_EVENTS.JOB_UUID;
    }

    @Override
    public Field<String> field5() {
        return JobEvents.JOB_EVENTS.EVENT_DETAIL;
    }

    @Override
    public Field<String> field6() {
        return JobEvents.JOB_EVENTS.OTH_UUID;
    }

    @Override
    public Field<String> field7() {
        return JobEvents.JOB_EVENTS.DESCRIPTION;
    }

    @Override
    public Field<String> field8() {
        return JobEvents.JOB_EVENTS.TENANT;
    }

    @Override
    public Long component1() {
        return getId();
    }

    @Override
    public JobEventType component2() {
        return getEvent();
    }

    @Override
    public LocalDateTime component3() {
        return getCreated();
    }

    @Override
    public String component4() {
        return getJobUuid();
    }

    @Override
    public String component5() {
        return getEventDetail();
    }

    @Override
    public String component6() {
        return getOthUuid();
    }

    @Override
    public String component7() {
        return getDescription();
    }

    @Override
    public String component8() {
        return getTenant();
    }

    @Override
    public Long value1() {
        return getId();
    }

    @Override
    public JobEventType value2() {
        return getEvent();
    }

    @Override
    public LocalDateTime value3() {
        return getCreated();
    }

    @Override
    public String value4() {
        return getJobUuid();
    }

    @Override
    public String value5() {
        return getEventDetail();
    }

    @Override
    public String value6() {
        return getOthUuid();
    }

    @Override
    public String value7() {
        return getDescription();
    }

    @Override
    public String value8() {
        return getTenant();
    }

    @Override
    public JobEventsRecord value1(Long value) {
        setId(value);
        return this;
    }

    @Override
    public JobEventsRecord value2(JobEventType value) {
        setEvent(value);
        return this;
    }

    @Override
    public JobEventsRecord value3(LocalDateTime value) {
        setCreated(value);
        return this;
    }

    @Override
    public JobEventsRecord value4(String value) {
        setJobUuid(value);
        return this;
    }

    @Override
    public JobEventsRecord value5(String value) {
        setEventDetail(value);
        return this;
    }

    @Override
    public JobEventsRecord value6(String value) {
        setOthUuid(value);
        return this;
    }

    @Override
    public JobEventsRecord value7(String value) {
        setDescription(value);
        return this;
    }

    @Override
    public JobEventsRecord value8(String value) {
        setTenant(value);
        return this;
    }

    @Override
    public JobEventsRecord values(Long value1, JobEventType value2, LocalDateTime value3, String value4, String value5, String value6, String value7, String value8) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        value7(value7);
        value8(value8);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached JobEventsRecord
     */
    public JobEventsRecord() {
        super(JobEvents.JOB_EVENTS);
    }

    /**
     * Create a detached, initialised JobEventsRecord
     */
    public JobEventsRecord(Long id, JobEventType event, LocalDateTime created, String jobUuid, String eventDetail, String othUuid, String description, String tenant) {
        super(JobEvents.JOB_EVENTS);

        setId(id);
        setEvent(event);
        setCreated(created);
        setJobUuid(jobUuid);
        setEventDetail(eventDetail);
        setOthUuid(othUuid);
        setDescription(description);
        setTenant(tenant);
    }
}

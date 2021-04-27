/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records;


import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobBlocked;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;

import java.time.LocalDateTime;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record6;
import org.jooq.Row6;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class JobBlockedRecord extends UpdatableRecordImpl<JobBlockedRecord> implements Record6<Integer, Integer, LocalDateTime, JobStatusType, String, String> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.job_blocked.id</code>.
     */
    public void setId(Integer value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.job_blocked.id</code>.
     */
    public Integer getId() {
        return (Integer) get(0);
    }

    /**
     * Setter for <code>public.job_blocked.recovery_id</code>.
     */
    public void setRecoveryId(Integer value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.job_blocked.recovery_id</code>.
     */
    public Integer getRecoveryId() {
        return (Integer) get(1);
    }

    /**
     * Setter for <code>public.job_blocked.created</code>.
     */
    public void setCreated(LocalDateTime value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.job_blocked.created</code>.
     */
    public LocalDateTime getCreated() {
        return (LocalDateTime) get(2);
    }

    /**
     * Setter for <code>public.job_blocked.success_status</code>.
     */
    public void setSuccessStatus(JobStatusType value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.job_blocked.success_status</code>.
     */
    public JobStatusType getSuccessStatus() {
        return (JobStatusType) get(3);
    }

    /**
     * Setter for <code>public.job_blocked.job_uuid</code>.
     */
    public void setJobUuid(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.job_blocked.job_uuid</code>.
     */
    public String getJobUuid() {
        return (String) get(4);
    }

    /**
     * Setter for <code>public.job_blocked.status_message</code>.
     */
    public void setStatusMessage(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.job_blocked.status_message</code>.
     */
    public String getStatusMessage() {
        return (String) get(5);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Integer> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record6 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row6<Integer, Integer, LocalDateTime, JobStatusType, String, String> fieldsRow() {
        return (Row6) super.fieldsRow();
    }

    @Override
    public Row6<Integer, Integer, LocalDateTime, JobStatusType, String, String> valuesRow() {
        return (Row6) super.valuesRow();
    }

    @Override
    public Field<Integer> field1() {
        return JobBlocked.JOB_BLOCKED.ID;
    }

    @Override
    public Field<Integer> field2() {
        return JobBlocked.JOB_BLOCKED.RECOVERY_ID;
    }

    @Override
    public Field<LocalDateTime> field3() {
        return JobBlocked.JOB_BLOCKED.CREATED;
    }

    @Override
    public Field<JobStatusType> field4() {
        return JobBlocked.JOB_BLOCKED.SUCCESS_STATUS;
    }

    @Override
    public Field<String> field5() {
        return JobBlocked.JOB_BLOCKED.JOB_UUID;
    }

    @Override
    public Field<String> field6() {
        return JobBlocked.JOB_BLOCKED.STATUS_MESSAGE;
    }

    @Override
    public Integer component1() {
        return getId();
    }

    @Override
    public Integer component2() {
        return getRecoveryId();
    }

    @Override
    public LocalDateTime component3() {
        return getCreated();
    }

    @Override
    public JobStatusType component4() {
        return getSuccessStatus();
    }

    @Override
    public String component5() {
        return getJobUuid();
    }

    @Override
    public String component6() {
        return getStatusMessage();
    }

    @Override
    public Integer value1() {
        return getId();
    }

    @Override
    public Integer value2() {
        return getRecoveryId();
    }

    @Override
    public LocalDateTime value3() {
        return getCreated();
    }

    @Override
    public JobStatusType value4() {
        return getSuccessStatus();
    }

    @Override
    public String value5() {
        return getJobUuid();
    }

    @Override
    public String value6() {
        return getStatusMessage();
    }

    @Override
    public JobBlockedRecord value1(Integer value) {
        setId(value);
        return this;
    }

    @Override
    public JobBlockedRecord value2(Integer value) {
        setRecoveryId(value);
        return this;
    }

    @Override
    public JobBlockedRecord value3(LocalDateTime value) {
        setCreated(value);
        return this;
    }

    @Override
    public JobBlockedRecord value4(JobStatusType value) {
        setSuccessStatus(value);
        return this;
    }

    @Override
    public JobBlockedRecord value5(String value) {
        setJobUuid(value);
        return this;
    }

    @Override
    public JobBlockedRecord value6(String value) {
        setStatusMessage(value);
        return this;
    }

    @Override
    public JobBlockedRecord values(Integer value1, Integer value2, LocalDateTime value3, JobStatusType value4, String value5, String value6) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached JobBlockedRecord
     */
    public JobBlockedRecord() {
        super(JobBlocked.JOB_BLOCKED);
    }

    /**
     * Create a detached, initialised JobBlockedRecord
     */
    public JobBlockedRecord(Integer id, Integer recoveryId, LocalDateTime created, JobStatusType successStatus, String jobUuid, String statusMessage) {
        super(JobBlocked.JOB_BLOCKED);

        setId(id);
        setRecoveryId(recoveryId);
        setCreated(created);
        setSuccessStatus(successStatus);
        setJobUuid(jobUuid);
        setStatusMessage(statusMessage);
    }
}

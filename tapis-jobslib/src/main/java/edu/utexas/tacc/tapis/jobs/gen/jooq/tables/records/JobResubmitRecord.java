/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records;


import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobResubmit;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Row3;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class JobResubmitRecord extends UpdatableRecordImpl<JobResubmitRecord> implements Record3<Integer, String, String> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.job_resubmit.id</code>.
     */
    public void setId(Integer value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.job_resubmit.id</code>.
     */
    public Integer getId() {
        return (Integer) get(0);
    }

    /**
     * Setter for <code>public.job_resubmit.job_uuid</code>.
     */
    public void setJobUuid(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.job_resubmit.job_uuid</code>.
     */
    public String getJobUuid() {
        return (String) get(1);
    }

    /**
     * Setter for <code>public.job_resubmit.job_definition</code>.
     */
    public void setJobDefinition(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.job_resubmit.job_definition</code>.
     */
    public String getJobDefinition() {
        return (String) get(2);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Integer> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record3 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row3<Integer, String, String> fieldsRow() {
        return (Row3) super.fieldsRow();
    }

    @Override
    public Row3<Integer, String, String> valuesRow() {
        return (Row3) super.valuesRow();
    }

    @Override
    public Field<Integer> field1() {
        return JobResubmit.JOB_RESUBMIT.ID;
    }

    @Override
    public Field<String> field2() {
        return JobResubmit.JOB_RESUBMIT.JOB_UUID;
    }

    @Override
    public Field<String> field3() {
        return JobResubmit.JOB_RESUBMIT.JOB_DEFINITION;
    }

    @Override
    public Integer component1() {
        return getId();
    }

    @Override
    public String component2() {
        return getJobUuid();
    }

    @Override
    public String component3() {
        return getJobDefinition();
    }

    @Override
    public Integer value1() {
        return getId();
    }

    @Override
    public String value2() {
        return getJobUuid();
    }

    @Override
    public String value3() {
        return getJobDefinition();
    }

    @Override
    public JobResubmitRecord value1(Integer value) {
        setId(value);
        return this;
    }

    @Override
    public JobResubmitRecord value2(String value) {
        setJobUuid(value);
        return this;
    }

    @Override
    public JobResubmitRecord value3(String value) {
        setJobDefinition(value);
        return this;
    }

    @Override
    public JobResubmitRecord values(Integer value1, String value2, String value3) {
        value1(value1);
        value2(value2);
        value3(value3);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached JobResubmitRecord
     */
    public JobResubmitRecord() {
        super(JobResubmit.JOB_RESUBMIT);
    }

    /**
     * Create a detached, initialised JobResubmitRecord
     */
    public JobResubmitRecord(Integer id, String jobUuid, String jobDefinition) {
        super(JobResubmit.JOB_RESUBMIT);

        setId(id);
        setJobUuid(jobUuid);
        setJobDefinition(jobDefinition);
    }
}
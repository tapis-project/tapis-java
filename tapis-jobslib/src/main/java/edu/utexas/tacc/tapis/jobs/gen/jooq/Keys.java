/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.jobs.gen.jooq;


import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobBlocked;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobEvents;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobQueues;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobRecovery;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobResubmit;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.Jobs;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobBlockedRecord;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobEventsRecord;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobQueuesRecord;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobRecoveryRecord;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobResubmitRecord;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobsRecord;

import org.jooq.ForeignKey;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;


/**
 * A class modelling foreign key relationships and constraints of tables in
 * public.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<JobBlockedRecord> JOB_BLOCKED_PKEY = Internal.createUniqueKey(JobBlocked.JOB_BLOCKED, DSL.name("job_blocked_pkey"), new TableField[] { JobBlocked.JOB_BLOCKED.ID }, true);
    public static final UniqueKey<JobEventsRecord> JOB_EVENTS_PKEY = Internal.createUniqueKey(JobEvents.JOB_EVENTS, DSL.name("job_events_pkey"), new TableField[] { JobEvents.JOB_EVENTS.ID }, true);
    public static final UniqueKey<JobQueuesRecord> JOB_QUEUES_PKEY = Internal.createUniqueKey(JobQueues.JOB_QUEUES, DSL.name("job_queues_pkey"), new TableField[] { JobQueues.JOB_QUEUES.ID }, true);
    public static final UniqueKey<JobRecoveryRecord> JOB_RECOVERY_PKEY = Internal.createUniqueKey(JobRecovery.JOB_RECOVERY, DSL.name("job_recovery_pkey"), new TableField[] { JobRecovery.JOB_RECOVERY.ID }, true);
    public static final UniqueKey<JobResubmitRecord> JOB_RESUBMIT_PKEY = Internal.createUniqueKey(JobResubmit.JOB_RESUBMIT, DSL.name("job_resubmit_pkey"), new TableField[] { JobResubmit.JOB_RESUBMIT.ID }, true);
    public static final UniqueKey<JobsRecord> JOBS_PKEY = Internal.createUniqueKey(Jobs.JOBS, DSL.name("jobs_pkey"), new TableField[] { Jobs.JOBS.ID }, true);

    // -------------------------------------------------------------------------
    // FOREIGN KEY definitions
    // -------------------------------------------------------------------------

    public static final ForeignKey<JobBlockedRecord, JobRecoveryRecord> JOB_BLOCKED__JOB_BLOCKED_RECOVERY_ID_FKEY = Internal.createForeignKey(JobBlocked.JOB_BLOCKED, DSL.name("job_blocked_recovery_id_fkey"), new TableField[] { JobBlocked.JOB_BLOCKED.RECOVERY_ID }, Keys.JOB_RECOVERY_PKEY, new TableField[] { JobRecovery.JOB_RECOVERY.ID }, true);
}

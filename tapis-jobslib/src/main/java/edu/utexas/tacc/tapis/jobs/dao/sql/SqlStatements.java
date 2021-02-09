package edu.utexas.tacc.tapis.jobs.dao.sql;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;

public class SqlStatements 
{
	/* ---------------------------------------------------------------------- */
	/* any table:                                                             */
	/* ---------------------------------------------------------------------- */
	public static final String SELECT_1 = "SELECT 1 FROM :table LIMIT 1";    

	/* ---------------------------------------------------------------------- */
	/* jobs table:                                                            */
	/* ---------------------------------------------------------------------- */
    public static final String SELECT_JOBS =
        "SELECT id, name, owner, tenant, description, status, "
        	+ "last_message, created, ended, last_updated, uuid, app_id, app_version, "
        	+ "archive_on_app_error, dynamic_exec_system, exec_system_id, exec_system_exec_dir, "
        	+ "exec_system_input_dir, exec_system_output_dir, exec_system_logical_queue, "
        	+ "archive_system_id, archive_system_dir, "
        	+ "dtn_system_id, dtn_mount_source_path, dtn_mount_point, "
        	+ "node_count, cores_per_node, memory_mb, max_minutes, file_inputs, parameter_set, "
        	+ "exec_system_constraints, subscriptions, "
        	+ "blocked_count, remote_job_id, remote_job_id2, "
        	+ "remote_outcome, remote_result_info, remote_queue, remote_submitted, "
        	+ "remote_started, remote_ended, remote_submit_retries, remote_checks_success, "
        	+ "remote_checks_failed, remote_last_status_check, "
        	+ "input_transaction_id, input_correlation_id, archive_transaction_id, archive_correlation_id, "
        	+ "tapis_queue, visible, createdby, createdby_tenant, tags"
        + " FROM jobs ORDER BY id";

    public static final String SELECT_JOBS_BY_UUID =
        "SELECT id, name, owner, tenant, description, status, "
        	+ "last_message, created, ended, last_updated, uuid, app_id, app_version, "
        	+ "archive_on_app_error, dynamic_exec_system, exec_system_id, exec_system_exec_dir, "
            + "exec_system_input_dir, exec_system_output_dir, exec_system_logical_queue, "
            + "archive_system_id, archive_system_dir, "
            + "dtn_system_id, dtn_mount_source_path, dtn_mount_point, "
            + "node_count, cores_per_node, memory_mb, max_minutes, file_inputs, parameter_set, "
            + "exec_system_constraints, subscriptions, "
            + "blocked_count, remote_job_id, remote_job_id2, "
            + "remote_outcome, remote_result_info, remote_queue, remote_submitted, "
            + "remote_started, remote_ended, remote_submit_retries, remote_checks_success, "
            + "remote_checks_failed, remote_last_status_check, "
            + "input_transaction_id, input_correlation_id, archive_transaction_id, archive_correlation_id, "
            + "tapis_queue, visible, createdby, createdby_tenant, tags"
        + " FROM jobs"
        + " WHERE uuid = ?";
    
    public static final String SELECT_JOBS_STATUS_BY_UUID =
            "SELECT uuid, id,  owner, tenant, status, createdby, visible, createdby_tenant"
            + " FROM jobs"
            + " WHERE uuid = ?";
    
    // All of the job fields except:
    // 
    //	- id, ended, blocked_count, 12 remote_*, visible
    //
    public static final String CREATE_JOB = 
    		"INSERT INTO jobs ("
    		+ "name, owner, tenant, description, status, "
        	+ "last_message, created, last_updated, uuid, app_id, app_version, "
        	+ "archive_on_app_error, dynamic_exec_system, exec_system_id, exec_system_exec_dir, "
            + "exec_system_input_dir, exec_system_output_dir, exec_system_logical_queue, "
            + "archive_system_id, archive_system_dir, "
            + "dtn_system_id, dtn_mount_source_path, dtn_mount_point, "
            + "node_count, cores_per_node, memory_mb, max_minutes, file_inputs, parameter_set, "
            + "exec_system_constraints, subscriptions, "
            + "tapis_queue, createdby, createdby_tenant, tags) "
    		+ "VALUES (?, ?, ?, ?, ?::job_status_enum, "
    		+ "?, ?, ?, ?, ?, ?, "
    		+ "?, ?, ?, ?, "
    		+ "?, ?, ?, "
    		+ "?, ?, "
    		+ "?, ?, ?, "
    		+ "?, ?, ?, ?, ?::json, ?::json, ?, ?::json, ?, ?, ?, ?)"; 

    public static final String SELECT_JOB_STATUS_FOR_UPDATE = 
        "SELECT status FROM jobs WHERE tenant_id = ? AND uuid = ? FOR UPDATE";
    
    public static final String UPDATE_JOB_STATUS =
        "UPDATE jobs SET status = ?, last_message = ?, last_updated = ?, blocked_count = blocked_count + ?"
        + " WHERE tenant_id = ? AND uuid = ?";
    
    public static final String UPDATE_JOB_ENDED =
        "UPDATE jobs SET ended = ? WHERE ended IS NULL AND uuid = ?";
      
    public static final String UPDATE_REMOTE_STARTED = 
        "UPDATE jobs SET remote_started = ? WHERE remote_started IS NULL AND uuid = ?";
    
    public static final String UPDATE_JOB_LAST_MESSAGE =
        "UPDATE aloe_jobs SET last_message = ?, last_updated = ?"
        + " WHERE tenant = ? AND id = ?";
      
	/* ---------------------------------------------------------------------- */
	/* job_resubmit table:                                                    */
	/* ---------------------------------------------------------------------- */
    public static final String SELECT_JOBRESUBMIT =
        "SELECT id, job_uuid, job_definition"
        + " FROM job_resubmit ORDER BY id";
    
    public static final String SELECT_JOBRESUBMIT_BY_UUID =
        "SELECT id, job_uuid, job_definition"
        + " FROM job_resubmit"
        + " WHERE uuid = ?";
        
    public static final String CREATE_JOBRESUBMIT =
        "INSERT INTO job_resubmit (job_uuid, job_definition) "
        + "VALUES (?, ?)";
        
	/* ---------------------------------------------------------------------- */
	/* job_recovery table:                                                    */
	/* ---------------------------------------------------------------------- */
    public static final String SELECT_JOBRECOVERY =
        "SELECT id, tenant_id, condition_code, tester_type, tester_parms, "
            + "policy_type, policy_parms, num_attempts, next_attempt, created, "
            + "last_updated, tester_hash"
        + " FROM job_recovery ORDER BY id";

	/* ---------------------------------------------------------------------- */
	/* job_blocked table:                                                     */
	/* ---------------------------------------------------------------------- */
    public static final String SELECT_JOBBLOCKED =
        "SELECT id, recovery_id, created, success_status, job_uuid, status_message"
        + " FROM job_blocked ORDER BY id";
    
	/* ---------------------------------------------------------------------- */
	/* job_queues table:                                                      */
	/* ---------------------------------------------------------------------- */
    public static final String SELECT_JOBQUEUES_BY_PRIORITY_DESC =
        "SELECT id, name, priority, filter, uuid, created, last_updated"
        + " FROM job_queues ORDER BY priority desc";

    public static final String SELECT_JOBQUEUE_BY_NAME =
            "SELECT id, name, priority, filter, uuid, created, last_updated"
            + " FROM job_queues WHERE name = ?";

    public static final String CREATE_JOBQUEUE =
            "INSERT into job_queues (name, priority, filter, uuid, created, last_updated)"
            + " VALUES (?, ?, ?, ?, ?, ?)";

	/* ---------------------------------------------------------------------- */
	/* job_events table:                                                      */
	/* ---------------------------------------------------------------------- */
    public static final String CREATE_JOB_EVENT = 
        "INSERT INTO job_events (event, created, job_uuid, job_status, oth_uuid, description) "
        + "VALUES (?::job_event_enum, ?, ?, ?::job_status_enum, ?, ?)";
    
    public static final String SELECT_JOBEVENTS =
        "SELECT id, event, created, job_uuid, job_status, oth_uuid, description"
        + " FROM job_events ORDER BY id";

}

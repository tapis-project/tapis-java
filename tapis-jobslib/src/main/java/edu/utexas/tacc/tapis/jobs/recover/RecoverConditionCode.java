package edu.utexas.tacc.tapis.jobs.recover;

/** This enumeration defines transient conditions that would cause a job
 * to be put into a BLOCKED state.  Once the condition clears the jobs
 * will be requeued so that execution can be resumed.
 * 
 * @author rcardone
 */
public enum RecoverConditionCode 
{
    SYSTEM_NOT_AVAILABLE,
    APPLICATION_NOT_AVAILABLE,
    SERVICE_CONNECTION_FAILURE,
    CONNECTION_FAILURE,
    TRANSMISSION_FAILURE,
    DATABASE_NOT_AVAILABLE,
    QUOTA_EXCEEDED,
    JOB_SUSPENDED,
    AUTHENTICATION_FAILED,
    FIRST_AUTHENTICATION_FAILED
}

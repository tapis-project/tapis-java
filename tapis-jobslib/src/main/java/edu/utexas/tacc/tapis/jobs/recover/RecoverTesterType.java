package edu.utexas.tacc.tapis.jobs.recover;

/** This enumeration defines the tester that should be run
 * to determine if the related condition is no longer blocking
 * a job's progress.
 * 
 * @author rcardone
 */
public enum RecoverTesterType 
{
    DEFAULT_SYSTEM_AVAILABLE_TESTER,
    DEFAULT_APPLICATION_TESTER,
    DEFAULT_CONNECTION_TESTER,
    DEFAULT_TRANSMISSION_TESTER,
    DEFAULT_DATABASE_TESTER,
    DEFAULT_QUOTA_TESTER,
    DEFAULT_JOB_SUSPENDED_TESTER,
    DEFAULT_AUTHENTICATION_TESTER
}

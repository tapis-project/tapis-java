package edu.utexas.tacc.tapis.jobs.exceptions.recoverable;

/** Information used to construct or decode recoverable exceptions.
 * 
 * @author rcardone
 */
public final class JobRecoveryDefinitions 
{
    // Keys assigned by workers and read by the recovery readers.
    public static final String BLOCKED_JOB_ACTIVITY = "BlockedJobActivity"; // see enum below for values
    
    // Let's indicate exactly what we were doing when we ran into a blocking condition.
    // This information is added to the state saved in some recoverable exceptions.
    public enum BlockedJobActivity {CHECK_SYSTEMS, CHECK_QUOTA, PROCESSING_INPUTS, STAGING_INPUTS, STAGING_JOB,
                                    SUBMITTING, QUEUED, RUNNING, ARCHIVING}
}

package edu.utexas.tacc.tapis.jobs.statemachine;

/** The events recognized by the Jobs FSM.
 * 
 * @author rcardone
 */
public enum JobFSMEvents 
{
    // Events with the format T0_<status> 
    // request a change to the target status
    TO_PENDING,
    TO_PROCESSING_INPUTS,
    
    TO_STAGING_INPUTS,
    TO_STAGING_JOB,
    TO_SUBMITTING_JOB,
    
    TO_QUEUED,
    TO_RUNNING,
    TO_ARCHIVING,
    
    TO_BLOCKED,
    TO_PAUSED,
    
    TO_FINISHED,
    TO_CANCELLED,
    TO_FAILED
}

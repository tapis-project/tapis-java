package edu.utexas.tacc.tapis.jobs.recover;

/** This enumeration defines the policy used to determine 
 * when the next availability test should be run.  Policies
 * may have a time limit on how long a condition will be
 * tested before terminating the job. 
 * 
 * @author rcardone
 */
public enum RecoverPolicyType 
{
    STEPWISE_BACKOFF,
    CONSTANT_BACKOFF
}

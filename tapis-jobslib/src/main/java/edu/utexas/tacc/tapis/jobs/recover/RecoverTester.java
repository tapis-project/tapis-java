package edu.utexas.tacc.tapis.jobs.recover;

import java.util.Map;

import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;

public interface RecoverTester 
{
    /** Test whether the condition that has blocked one or more 
     * jobs has been cleared.
     * 
     * @param testerParameters parameter used to test resource availability
     * @return 0 if no jobs can be unblocked, a positive integer to indicate
     *         the number of jobs to be rescheduled
     * @throws JobRecoveryAbortException on errors that indicate no recovery 
     *          will ever be possible
     */
    int canUnblock(Map<String,String> testerParameters)
     throws JobRecoveryAbortException;
}

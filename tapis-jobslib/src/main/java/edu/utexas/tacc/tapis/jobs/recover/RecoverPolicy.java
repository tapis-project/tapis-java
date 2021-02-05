package edu.utexas.tacc.tapis.jobs.recover;

public interface RecoverPolicy 
{
    /* ********************************************************************** */
    /*                                 Enums                                  */
    /* ********************************************************************** */
    // The monitor sets a reason code whenever a null wait time is returned
    // from millisToWait.  This code indicates the reason why no more 
    // monitoring attempts should be made.
    enum ReasonCode {TIME_EXPIRED, TOO_MANY_ATTEMPTS}
    
    /* ********************************************************************** */
    /*                               Methods                                  */
    /* ********************************************************************** */
    /** Calculate the time that the next availability test should be
     * made for the blocked job.  The test will determine if the 
     * blocking condition is has been lifted.  Return null if the policy has 
     * expired because all steps have completed or because the overall time 
     * limit has been exceeded.
     * 
     * Whenever null is return, the reason code field MUST be set to a non-null
     * value. 
     * 
     * @param numAttempts the number of failed attempts to access the 
     *                    unavailable resource
     * @param policyParameters parameters used to calculate the next test time
     * @return the milliseconds to wait before the next attempt should be made or
     *         null if no more attempts should be made.
     */
    Long millisToWait();
    
    /** Return the expiration reason code when null is returned by millisToWait().
     * 
     * @return the reason why the policy expired
     */
    ReasonCode getReasonCode();
}

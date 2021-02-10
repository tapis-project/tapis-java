package edu.utexas.tacc.tapis.jobs.monitors.policies;

public interface MonitorPolicy 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // The millisecond duration is the time to sleep before returning control
    // to the main monitoring loop.  Note that this time is effectively added
    // to the first step time.  The retry number is how many times we'll
    // test for the possible queuing race condition.
    static final long INITIAL_QUEUED_MILLIS      = 1000L; 
    static final int  MAX_INITIAL_QUEUED_RETRIES = 15;
    
    // The default step size (i.e., number of milliseconds) cutoff 
    // after which connections are dropped after each use.
    static final long DEFAULT_STEP_CONN_CLOSE_MS = 60000; // 1 minute
    
    // The number of minutes in which an unbroken sequence of monitoring
    // attempts failures causes a timeout.
    static final long DEFAULT_CONSECUTIVE_FAILURE_MINUTES = 180; // 3 hours 
    
    /* ********************************************************************** */
    /*                                 Enums                                  */
    /* ********************************************************************** */
    // The monitor sets a reason code whenever a null wait time is returned
    // from millisToWait.  This code indicates the reason why no more 
    // monitoring attempts should be made.
    enum ReasonCode {TIME_EXPIRED, TOO_MANY_ATTEMPTS, TOO_MANY_FAILURES}
    
    /* ********************************************************************** */
    /*                                Methods                                 */
    /* ********************************************************************** */
    /** Calculate the time until the next monitor test should be
     * made for the remote job.  Null is returned if no more attempts
     * should be made.  When this occurs, the monitor's reason code is
     * set to indicate why no more attempts should be made. 
     * 
     * The lastAttemptFailed flag is set when the caller's last remote 
     * monitoring attempt failed.  A policy implementation can use this
     * information to limit the number of failures allowed.
     * 
     * @param lastAttemptFailed true if the last monitoring request to the 
     *         remote system failed, false otherwise
     * @return the number of milliseconds to wait before the next attempt 
     *         should be made or null if no more attempts should be made.
     */
    Long millisToWait(boolean lastAttemptFailed);
    
    /** Return the reason why a null wait time was returned on the last call
     * to millisToWait.  
     * 
     * @return the reason for returning a null wait time or
     *         null if the last wait time returned was not null. 
     */
    ReasonCode getReasonCode();

    /** Get the policy cutoff for connection closing.  If the time until the 
     * next scheduled status query is less than the configured amount, then the
     * policy recommends keeping the connection open.  Otherwise, the monitoring
     * code should close the connection to release resources on machines at both 
     * ends.
     * 
     * @return true if the policy recommends keeping the connection open, 
     *         false otherwise
     */
    boolean keepConnection();
    
    /** This method addresses a race condition in which the job was submitted to
     * a remote scheduler, but the remote job id does not immediately show up in
     * the response to a scheduler query.  Load on the remote system can delay
     * the scheduler's promptness in reporting recently submitted jobs.
     * 
     * This method handles the race condition by (1) detecting the initial queuing
     * condition, (2) sleeping for a configurable amount of time and (3) returning
     * true to indicate that another remote status check attempt should be tried.
     * 
     * If monitoring is not in the initial queuing condition, then this method has
     * no effect. 
     * 
     * @return true if the caller should retry the remote status check; false otherwise
     */
    boolean retryForInitialQueuing();
    
    /** Start the timer that enforces the maximum running time for a job on an execution 
     * system.  This method only needs to be called once when a job transitions from 
     * QUEUED to RUNNING status during monitoring.  If the job is already in RUNNING
     * when the monitoring policy instance is created, this method does not need to be
     * called.  This method is idempotent; all calls after the first call have no effect. 
     */
    void startJobExecutionTimer();
}

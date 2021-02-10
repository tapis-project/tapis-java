package edu.utexas.tacc.tapis.jobs.monitors.commands;

public interface JobMonitorCommand 
{
    /** Return the command used to monitor remote job execution.
     * This command is must be valid while the job is queued or 
     * running on the remote system, but may not return results 
     * after the remote job completes. 
     * 
     * @return the monitoring result
     */
    String getActiveJobCommand();
    
    /** The optional command used to monitor remote jobs after 
     * they have terminated.  Some schedulers require the use of
     * a different command to query the status of a job that has
     * completed execution.
     * 
     * @return the monitoring result
     */
    default String getInactiveJobCommand() {return null;}
}

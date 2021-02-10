package edu.utexas.tacc.tapis.jobs.monitors.parsers;

public interface JobMonitorResponseParser 
{
    /** Return the response type from the active job monitoring command.
     * 
     * @param rawOutput the active command's output
     * @return the response object
     */
    JobMonitorResponse parseActive(String rawOutput);
    
    /** Return the response type from the inactive job monitoring command.
     * 
     * @param rawOutput the inactive command's output
     * @return the response object or null if the inactive command is not supported
     */
    default JobMonitorResponse parseInActive(String rawOutput) {return null;}
}

package edu.utexas.tacc.tapis.jobs.monitors;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface JobMonitor 
{
    /** Continuously check the status of a queued job on the remote system 
     * and updates the job's database record.  The in-memory job object is 
     * also updated.  The job status is always updated when this method 
     * completes, whether completion is by returning or by throwing an 
     * exception.  
     * 
     * @throws TapisException on monitoring error
     */
    void monitorQueuedJob() throws TapisException;
    
    /** Continuously check the status of a running job on the remote system 
     * and updates the job's database record.  The in-memory job object is 
     * also updated.  The job status is always updated when this method 
     * completes, whether completion is by returning or by throwing an 
     * exception.  
     * 
     * @throws TapisException on monitoring error
     */
    void monitorRunningJob() throws TapisException;
    
    /** Return the application exit code as reported by docker.  If no exit
     * code has been ascertained, null is returned. 
     * 
     * @return the application exit code or null
     */
    public String getExitCode();
    
    /** Close the connection to the monitored host if there are no other 
     * active channels using the connection.  
     */
    void closeConnection();
}
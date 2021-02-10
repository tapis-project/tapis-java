package edu.utexas.tacc.tapis.jobs.launchers.parsers;

public interface JobLaunchResult 
{
    /** Return an identifier or handle to the remote job.
     * The identifier format and usage is specific to the
     * runtime in which the remote job was launched.
     * 
     * @return the remote job id
     */
    String getRemoteJobId();
    
    /** Return the additional or auxiliary identifier that
     * some schedulers assign to a job. 
     * 
     * @return the auxiliary id or null if none is assigned
     */
    default String getRemoteJobId2() {return null;}
}

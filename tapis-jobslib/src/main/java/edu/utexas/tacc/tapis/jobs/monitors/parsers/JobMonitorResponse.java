package edu.utexas.tacc.tapis.jobs.monitors.parsers;

public interface JobMonitorResponse 
{
    /** Return an identifier or handle to the remote job.
     * The identifier format and usage is specific to the
     * runtime in which the remote job was launched.
     * 
     * @return the remote job id
     */
    JobRemoteStatus getResponseType();
}

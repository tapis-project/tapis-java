package edu.utexas.tacc.tapis.jobs.launchers;

import edu.utexas.tacc.tapis.jobs.launchers.parsers.JobLaunchResult;

public interface JobLauncher 
{
    /** Launch a remote job on an execution system and return all information
     * needed to monitor and interact with the remote job.
     * 
     * @return reference information for remote job access 
     */
    JobLaunchResult launch();
}

package edu.utexas.tacc.tapis.jobs.stager;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface JobExecStager 
{
    /** Create a the execution script that launches a job. */
    String generateWrapperScript() throws TapisException;
    
    /** Create environment variable assignments for file. */
    String generateEnvVarFile() throws TapisException;
}

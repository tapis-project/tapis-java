package edu.utexas.tacc.tapis.jobs.stager;

public interface JobExecStager 
{
    /** Create a the execution script that launches a job. */
    String generateWrapperScript();
}

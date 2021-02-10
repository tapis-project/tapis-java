package edu.utexas.tacc.tapis.jobs.launchers.parsers;

public interface JobLaunchResultParser 
{
    JobLaunchResult parse(String rawOutput);
}

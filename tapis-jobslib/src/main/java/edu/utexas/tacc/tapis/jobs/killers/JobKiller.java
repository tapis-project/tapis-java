package edu.utexas.tacc.tapis.jobs.killers;

public interface JobKiller 
{
	/** Run the remote termination command. */
	public void attack();
}

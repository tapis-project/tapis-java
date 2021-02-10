package edu.utexas.tacc.tapis.jobs.monitors;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface JobMonitor 
{
	/** Continuously check the status of the job on the remote system and 
	 * updates the job's database record.  The in-memory job object is 
	 * also updated.  The job status is always updated when this method 
	 * completes, whether completion is by returning or by throwing an 
	 * exception.  
	 * 
	 * @throws TapisException on monitoring error
	 */
	public void monitor() throws TapisException;
	
	/** This method determines whether the result of a monitoring 
	 * request can be empty or not.  When true is returned, empty
	 * results from a monitor query do not cause an exception to
	 * be thrown.  When false is returned, the remote client code
	 * considers an empty response to be an error and throws an
	 * exception.
	 * 
	 * @return true to allow empty monitoring results, false otherwise
	 */
	public boolean allowEmptyResult();
}
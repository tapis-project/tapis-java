package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobMonitorResponseException extends JobException {

	public JobMonitorResponseException(String message)
	{
		super(message);
	}

	public JobMonitorResponseException(String message, Throwable cause)
	{
		super(message, cause);
	}
}

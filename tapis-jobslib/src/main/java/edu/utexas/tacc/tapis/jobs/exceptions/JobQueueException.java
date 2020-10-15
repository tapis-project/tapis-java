package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobQueueException extends JobException {

	public JobQueueException(String message)
	{
		super(message);
	}

	public JobQueueException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

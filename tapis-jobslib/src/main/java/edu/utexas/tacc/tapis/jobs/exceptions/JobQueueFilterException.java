package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobQueueFilterException extends JobQueueException {

	public JobQueueFilterException(String message)
	{
		super(message);
	}

	public JobQueueFilterException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

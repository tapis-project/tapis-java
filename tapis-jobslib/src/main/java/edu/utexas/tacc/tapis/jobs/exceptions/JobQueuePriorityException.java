package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobQueuePriorityException extends JobQueueException {

	public JobQueuePriorityException(String message)
	{
		super(message);
	}

	public JobQueuePriorityException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

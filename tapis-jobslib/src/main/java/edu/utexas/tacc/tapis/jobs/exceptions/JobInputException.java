package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobInputException extends JobException {

	public JobInputException(String message)
	{
		super(message);
	}

	public JobInputException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

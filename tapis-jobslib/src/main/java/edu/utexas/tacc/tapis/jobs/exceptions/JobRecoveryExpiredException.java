package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobRecoveryExpiredException extends JobRecoveryAbortException {

	public JobRecoveryExpiredException(String message)
	{
		super(message);
	}

	public JobRecoveryExpiredException(String message, Throwable cause)
	{
		super(message, cause);
	}
}

package edu.utexas.tacc.tapis.jobs.exceptions;

/** A subtype of the general job exception.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public class JobRecoveryAbortException extends JobException {

	public JobRecoveryAbortException(String message)
	{
		super(message);
	}

	public JobRecoveryAbortException(String message, Throwable cause)
	{
		super(message, cause);
	}
}

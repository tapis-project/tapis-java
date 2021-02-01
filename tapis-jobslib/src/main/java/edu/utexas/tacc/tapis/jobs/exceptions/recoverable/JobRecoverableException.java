package edu.utexas.tacc.tapis.jobs.exceptions.recoverable;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;

/** JOB WORKER RECOVERABLE EXCEPTION 
 * 
 * A subtype of the general job exception that indicates a worker
 * encountered a problem during job processing.  Neither the job
 * nor the worker is bad, but the job cannot be processed at this
 * time.  This exception, for instance, can be used to indicate a 
 * transient problem such as a database or network connectivity
 * issue.
 * 
 * @author rcardone
 */
@SuppressWarnings("serial")
public abstract class JobRecoverableException extends JobException 
{
    // Fields
    public JobRecoverMsg jobRecoverMsg;

	public JobRecoverableException(JobRecoverMsg jobRecoverMsg, String message)
	{
		super(message);
		this.jobRecoverMsg = jobRecoverMsg;
	}

	public JobRecoverableException(JobRecoverMsg jobRecoverMsg, String message, Throwable cause)
	{
		super(message, cause);
		this.jobRecoverMsg = jobRecoverMsg;
	}

}

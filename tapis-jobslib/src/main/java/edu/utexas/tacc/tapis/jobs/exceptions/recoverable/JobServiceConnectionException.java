package edu.utexas.tacc.tapis.jobs.exceptions.recoverable;

import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;

public class JobServiceConnectionException
 extends JobRecoverableException
{
    private static final long serialVersionUID = 8970837627935647596L;
    
    public JobServiceConnectionException(JobRecoverMsg jobRecoverMsg, String message) {
        super(jobRecoverMsg, message);
    }
    public JobServiceConnectionException(JobRecoverMsg jobRecoverMsg, String message, Throwable cause)
    {
        super(jobRecoverMsg, message, cause);
    }
}

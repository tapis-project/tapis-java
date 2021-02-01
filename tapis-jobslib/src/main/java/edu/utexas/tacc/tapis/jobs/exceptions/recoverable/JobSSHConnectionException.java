package edu.utexas.tacc.tapis.jobs.exceptions.recoverable;

import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;

public class JobSSHConnectionException
 extends JobRecoverableException
{
    private static final long serialVersionUID = -5372476084405050034L;
    
    public JobSSHConnectionException(JobRecoverMsg jobRecoverMsg, String message) {
        super(jobRecoverMsg, message);
    }
    public JobSSHConnectionException(JobRecoverMsg jobRecoverMsg, String message, Throwable cause)
    {
        super(jobRecoverMsg, message, cause);
    }
}

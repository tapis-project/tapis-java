package edu.utexas.tacc.tapis.jobs.exceptions.recoverable;

import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;

public class JobSystemAvailableException
 extends JobRecoverableException
{
    private static final long serialVersionUID = 981133665917190966L;
    
    public JobSystemAvailableException(JobRecoverMsg jobRecoverMsg, String message) {
        super(jobRecoverMsg, message);
    }
    public JobSystemAvailableException(JobRecoverMsg jobRecoverMsg, String message, Throwable cause)
    {
        super(jobRecoverMsg, message, cause);
    }

}

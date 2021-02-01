package edu.utexas.tacc.tapis.jobs.exceptions.recoverable;

import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;

public class JobAppAvailableException
 extends JobRecoverableException
{
    private static final long serialVersionUID = 7407724521479303081L;
    
    public JobAppAvailableException(JobRecoverMsg jobRecoverMsg, String message) {
        super(jobRecoverMsg, message);
    }
    public JobAppAvailableException(JobRecoverMsg jobRecoverMsg, String message, Throwable cause)
    {
        super(jobRecoverMsg, message, cause);
    }
}

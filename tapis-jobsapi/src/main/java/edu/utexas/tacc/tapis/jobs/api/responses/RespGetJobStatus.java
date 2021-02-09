package edu.utexas.tacc.tapis.jobs.api.responses;


import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDisplay;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespGetJobStatus 
 extends RespAbstract
{
    public JobStatusDisplay result;
    public RespGetJobStatus(JobStatusDisplay jobstatus)  {result = jobstatus;}
}


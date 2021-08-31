package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.dto.JobCancelDisplay;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespCancelJob extends RespAbstract{
	public JobCancelDisplay result;
    public  RespCancelJob(JobCancelDisplay jobCancelMsg)  {result = jobCancelMsg;}
}

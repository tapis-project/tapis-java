package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.dto.JobShareDisplay;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public class RespShareJob extends RespAbstract
{
		public JobShareDisplay result;
	    public  RespShareJob(JobShareDisplay jobShareMsg)  {result = jobShareMsg;}
}

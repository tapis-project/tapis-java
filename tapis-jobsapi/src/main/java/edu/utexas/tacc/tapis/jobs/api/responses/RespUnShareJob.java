package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.dto.JobUnShareDisplay;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public class RespUnShareJob extends RespAbstract{
	public JobUnShareDisplay result;
    public  RespUnShareJob(JobUnShareDisplay jobUnShareMsg)  {result = jobUnShareMsg;} 

}

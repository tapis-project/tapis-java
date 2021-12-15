package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.dto.JobHideDisplay;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public class RespHideJob 
	extends RespAbstract{
		public JobHideDisplay result;
	    public  RespHideJob(JobHideDisplay jobHideMsg)  {result = jobHideMsg;}
}

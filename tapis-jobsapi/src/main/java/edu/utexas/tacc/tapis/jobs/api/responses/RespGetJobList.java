package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespGetJobList extends RespAbstract{
	
   public List<JobListDTO> result;
   public RespGetJobList(List<JobListDTO> jobList)  {
	    result = jobList;
	 
   }
}
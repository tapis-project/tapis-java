package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.dto.JobHistoryDisplayDTO;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;

public class RespJobHistory extends RespAbstract {
	public List<JobHistoryDisplayDTO> result;
	   public RespJobHistory(List<JobHistoryDisplayDTO> jobHists,int limit, String orderBy, int skip, String startAfter, int totalCount)  {
		    result = new ArrayList<>();
		    for (JobHistoryDisplayDTO jobHist : jobHists)
		    {
		      result.add(jobHist);
		    }

		    ResultListMetadata meta = new ResultListMetadata();
		    meta.recordCount = result.size();
		    meta.recordLimit = limit;
		    meta.recordsSkipped = skip;
		    meta.orderBy = orderBy;
		    meta.startAfter = startAfter;
		    meta.totalCount = totalCount;
		    metadata = meta;
		  }
}

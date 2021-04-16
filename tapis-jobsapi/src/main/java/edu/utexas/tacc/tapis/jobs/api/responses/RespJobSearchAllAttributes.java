package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.sharedapi.responses.RespSearch;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultMetadata;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultSearch;

public final class RespJobSearchAllAttributes extends RespSearch{
	public RespJobSearchAllAttributes(List<Job> jobList, int limit, String orderBy, int skip, String startAfter, int totalCount) {
		result = new ResultSearch();
	    result.search = jobList;
	    ResultMetadata tmpMeta = new ResultMetadata();
	    tmpMeta.recordCount = jobList.size();
	    tmpMeta.recordLimit = limit;
	    tmpMeta.recordsSkipped = skip;
	    tmpMeta.orderBy = orderBy;
	    tmpMeta.startAfter = startAfter;
	    tmpMeta.totalCount = totalCount;
	    result.metadata = tmpMeta;
	  }
	

}

package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.dto.JobShareListDTO;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;

public class RespGetJobShareList extends RespAbstract{

   public List<JobShareListDTO> result;
   public RespGetJobShareList(List<JobShareListDTO> shareList,int limit, String orderBy, int skip, String startAfter, int totalCount)  {
	    result = new ArrayList<>();
	    if(shareList != null) {
		    for (JobShareListDTO js : shareList)
		    {
		      result.add(js);
		    }
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
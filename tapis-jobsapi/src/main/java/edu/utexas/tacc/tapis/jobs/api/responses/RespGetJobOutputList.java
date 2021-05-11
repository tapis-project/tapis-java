package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;

public class RespGetJobOutputList extends RespAbstract{
	public List<FileInfo> result;
	   public RespGetJobOutputList(List<FileInfo> jobOutputList,int limit, int skip)  {
		    result = new ArrayList<>();
		    for (FileInfo fileInfo : jobOutputList)
		    {
		      result.add(fileInfo);
		    }

		    ResultListMetadata meta = new ResultListMetadata();
		    meta.recordCount = result.size();
		    meta.recordLimit = limit;
		    meta.recordsSkipped = skip;
		    metadata = meta;
		  }
}

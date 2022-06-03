package edu.utexas.tacc.tapis.jobs.api.responses;

import java.util.Collections;
import java.util.List;

import edu.utexas.tacc.tapis.notifications.client.gen.model.RespSubscriptions;
import edu.utexas.tacc.tapis.notifications.client.gen.model.TapisSubscription;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;


public final class RespGetSubscriptions 
 extends RespAbstract
{
    // public JsonArray result;
    public List<TapisSubscription> result;
    public RespGetSubscriptions(RespSubscriptions resp)  
    {
        // Make sure list is never null.
        result = resp.getResult();
        if (result == null) result = Collections.emptyList();
        
        // Assign metadata.
        var respMeta = resp.getMetadata();
	    ResultListMetadata meta = new ResultListMetadata();
	    meta.recordCount = result.size();
	    meta.recordLimit = respMeta.getRecordLimit();
	    meta.recordsSkipped = respMeta.getRecordsSkipped();
	    meta.orderBy = respMeta.getOrderBy();
	    meta.startAfter = respMeta.getStartAfter();
	    meta.totalCount = respMeta.getTotalCount();
	    metadata = meta;
	}
}
  
package edu.utexas.tacc.tapis.jobs.model.dto;

import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTask;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public class JobHistoryDisplayDTO {
	// Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobHistoryDisplayDTO.class);
    public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    
    private String        event;
    private Instant       created;
    private String        jobUuid;
    private String        jobStatus;
    private String        description;
    private String        transferTaskUuid;
    private JsonObject 	  transferSummary;
    
    public JobHistoryDisplayDTO(JobEvent jobEvent, String user, String tenant) throws TapisImplException{
    	
    	event =  jobEvent.getEvent().name();
    	jobStatus = jobEvent.getJobStatus().name();
    	created = jobEvent.getCreated();
    	description = jobEvent.getDescription();
    	transferTaskUuid = jobEvent.getOthUuid();
    	Gson gson = TapisGsonUtils.getGson();
    	FilesClient filesClient = null;
 		
		 filesClient = getServiceClient(FilesClient.class, user, tenant);
    	 transferSummary = gson.fromJson(TapisConstants.EMPTY_JSON, JsonObject.class);
    	if(jobEvent.getOthUuid()!= null) {
    		TransferTask transferTask = null;
    		
    		try {
    			transferTask= filesClient.getTransferTaskHistory(jobEvent.getOthUuid());
			} catch (TapisClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		if (transferTask!= null) {
    			transferSummary.addProperty("uuid",jobEvent.getOthUuid());
    			transferSummary.addProperty("status",transferTask.getStatus().getValue());
    			transferSummary.addProperty("estimatedTotalBytes",transferTask.getTotalBytesTransferred() );
    			transferSummary.addProperty("totalBytesTransferred",transferTask.getTotalBytesTransferred() );
    			transferSummary.addProperty("completeTransfers",transferTask.getCompleteTransfers());
    			transferSummary.addProperty("totalTransfers",transferTask.getTotalTransfers());
    			transferSummary.addProperty("created",transferTask.getCreated().toString());
    			transferSummary.addProperty("startTime",transferTask.getStartTime().toString());
    			transferSummary.addProperty("endTime",transferTask.getEndTime().toString());
    			transferSummary.addProperty("errorMessage",transferTask.getErrorMessage());
    			
    			
    		}
    		
    	}
    	
    	//summary.add("transferSummary", transferSummary);
    	//eventsSummary.add(summary);
   }
    
/* ---------------------------------------------------------------------------- */
/* getServiceClient:                                                            */
/* ---------------------------------------------------------------------------- */
/** Get a new or cached Service client.  This can only be called after
 * the request tenant and owner have be assigned.
 * 
 * @return the client
 * @throws TapisImplException
 */
public <T> T getServiceClient(Class<T> cls,  String user, String tenant) throws TapisImplException
{
    // Get the application client for this user@tenant.
    T client = null;
    try {
        client = ServiceClients.getInstance().getClient(
               user, tenant, cls);
    }
    catch (Exception e) {
        var serviceName = StringUtils.removeEnd(cls.getSimpleName(), "Client");
        String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", serviceName, 
                                     tenant, user);
        throw new TapisImplException(msg, e,  HTTP_INTERNAL_SERVER_ERROR );
    }

    return client;
}
}


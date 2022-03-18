package edu.utexas.tacc.tapis.jobs.model.dto;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public final class JobListDisplayDTO {

	// Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobListDisplayDTO.class);
    
    // Tenant base url when the tenant is misconfigured or non-existant.
    private static final String DUMMY_TENANT_BASE_URL = "https://dummy-tenant-url.fixme/";
    
	public String uuid; //uuid of the job
	public String name;
	public String owner;
	public String execSystemId;
	public String  archiveSystemId;
	public String appId;
	public String  appVersion;
	public Instant created;
	public JobStatusType status;
	public Instant remoteStarted; 
	public Instant ended;
	public String tenant;
	public Instant  lastUpdated;
	public JsonObject _links; 	// links to resources related to the job
	
	
    
    
	public JobListDisplayDTO(JobListDTO jobListObject, String tenantBaseURL) {
		
		// ------------------------- Check Input -------------------------
        // Return the empty display object if no job was provided. This should not happen.
        if (jobListObject == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobListDisplayDTO", "job");
            _log.error(msg);
            return;
        }
        
        uuid = jobListObject.getUuid();
        name = jobListObject.getName();
        owner = jobListObject.getOwner();
        execSystemId = jobListObject.getExecSystemId();
        archiveSystemId = jobListObject.getArchiveSystemId();
        appId = jobListObject.getAppId();
        appVersion= jobListObject.getAppVersion();
        created = jobListObject.getCreated();
        status = jobListObject.getStatus();
        remoteStarted = jobListObject.getRemoteStarted();
        ended = jobListObject.getEnded();
        tenant = jobListObject.getTenant();
        lastUpdated = jobListObject.getLastUpdated();
                
        Gson gson = TapisGsonUtils.getGson();
		JsonObject jsonObject = new JsonObject();
		_links = gson.fromJson(TapisConstants.EMPTY_JSON, JsonObject.class);
        // link to the job 
    	JsonObject self = new JsonObject();
    	self.addProperty("href", tenantBaseURL+ TapisConstants.JOBS_SERVICE + uuid);
    	jsonObject.add("self", self);
    	
    	// link archive data
    	JsonObject listing = new JsonObject();
    	listing.addProperty("href", tenantBaseURL+ TapisConstants.JOBS_SERVICE + uuid + "/output/list");
    	jsonObject.add("archiveData", listing);
	
		_links = jsonObject;
		
	}
 
}

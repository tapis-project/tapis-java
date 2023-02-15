package edu.utexas.tacc.tapis.jobs.api.responses;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;



import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;

public final class RespJobSearchSelectAttributes extends RespAbstract{
		public List<JsonObject> result; 
		public RespJobSearchSelectAttributes(List<Job> jobList, List<String>selectList,  int limit, 
				String orderBy, int skip, String startAfter, int totalCount) {
			result = new ArrayList<>();
		    if(jobList != null) {
				for (Job job : jobList)
			    {
			      result.add(addDisplayAttributes(job,selectList));
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
		
		public JsonObject addDisplayAttributes(Job job,List<String>selectList) {	
			String json = "";
	        String uuid = "";
	        JsonObject jObj = null;
	       
	        Gson gson = TapisGsonUtils.getGson();
        
            json = gson.toJson(job);
            uuid = job.getUuid();
            jObj = gson.fromJson(json, JsonObject.class);
            JsonObject jObj1 = gson.fromJson(json, JsonObject.class);;
            Set<String> jobKeySet = jObj1.keySet(); 
                       
            for(String jobKey : jobKeySet) {
            	System.out.println("jobKey="+ jobKey);
               if(!selectList.contains(jobKey)) {
            	   jObj.getAsJsonObject().remove(jobKey);
                }
            }      
            if(!selectList.contains("id") && !selectList.contains("uuid")) {
            	// Add the uuid anyway
                jObj.addProperty("uuid", uuid);
            }
           return jObj;            
        }
        
	}
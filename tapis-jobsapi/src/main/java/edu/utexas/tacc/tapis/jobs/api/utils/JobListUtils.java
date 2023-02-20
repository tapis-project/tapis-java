package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobListType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;

public class JobListUtils {
	  
    /* ---------------------------------------------------------------------------- */
    /* computeTotalCount:                                                           */
    /* ---------------------------------------------------------------------------- */
    public static int computeTotalCount(String obouser, String obotenant, List<String>searchList, List<OrderBy> orderByList, boolean shared) 
    		throws TapisImplException 
    {
    	var jobsImpl = JobsImpl.getInstance();
    	int computeTotalCount = jobsImpl.getJobsSearchListCountByUsername(obouser, 
			   obotenant, searchList, orderByList, shared);
    	return computeTotalCount;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getSharedJobUuids                                                            */
    /* ---------------------------------------------------------------------------- */
    
    public static List<String> getSharedJobUuids(boolean sharedWithMe, String user, String tenant) 
    throws TapisImplException
    {
    	// if jobs are shared with the user, list the shared job as well
	     
	       List<JobShared> getSharedList= new ArrayList<JobShared>();
	       var jobsImpl = JobsImpl.getInstance();
	       if(sharedWithMe) {
	    	 getSharedList = jobsImpl.getSharesJob(user, tenant);
	       }
	       List<String> sharedJobUuidsList = new ArrayList<String>();
	       for(JobShared js : getSharedList) {
	      	   if(js.getJobResource().name().startsWith("JOB_")) {
	      		 if(!sharedJobUuidsList.contains(js.getJobUuid())) {
	      		    sharedJobUuidsList.add(js.getJobUuid());
		    	   }
	  	       }
	       }
	       return sharedJobUuidsList;
	}
    
    /* ---------------------------------------------------------------------------- */
    /* computeSkip                                                                  */
    /* ---------------------------------------------------------------------------- */
    public static int computeSkip(String listType, String obouser, String obotenant, List<String> searchList, List<OrderBy> orderbyList, 
    		int skip, boolean notShared) throws TapisImplException {
    	int diffSkip = 0;
    	int totalCountOwner = 0;
    	if(!listType.equals(JobListType.SHARED_JOBS.name())) {
 		   totalCountOwner = JobListUtils.computeTotalCount(obouser, obotenant, searchList, orderbyList, notShared);
	  	   if(totalCountOwner <= skip) {
 			   diffSkip = skip - totalCountOwner;
 		   }
		 } else {
			  diffSkip = skip; 
		 }
    	return diffSkip;
    }
    /* ---------------------------------------------------------------------------- */
    /* computeSkip                                                                  */
    /* ---------------------------------------------------------------------------- */
    public static int computeSkipSqlStr(String listType, String obouser, String obotenant, String sqlStr, List<OrderBy> orderbyList, 
    		int skip, boolean notShared) throws TapisImplException {
    	int diffSkip = 0;
    	int totalCountOwner = 0;
    	if(!listType.equals(JobListType.SHARED_JOBS.name())) {
 		   //totalCountOwner = JobListUtils.computeTotalCount(obouser, obotenant, sqlStr, orderbyList, notShared);
 		   var jobsImpl = JobsImpl.getInstance();
 	       totalCountOwner = jobsImpl.getJobsSearchListCountByUsernameUsingSqlSearchStr(obouser, obotenant, sqlStr, orderbyList, notShared);
	  	   if(totalCountOwner <= skip) {
 			   diffSkip = skip - totalCountOwner;
 		   }
		 } else {
			  diffSkip = skip; 
		 }
    	return diffSkip;
    }
}

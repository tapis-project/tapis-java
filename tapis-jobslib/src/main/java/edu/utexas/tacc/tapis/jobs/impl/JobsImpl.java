package edu.utexas.tacc.tapis.jobs.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.jobs.dao.JobQueuesDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.events.JobEventManager;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.JobQueue;
import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.jobs.model.dto.JobHistoryDisplayDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobShareListDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.jobs.model.submit.JobSharedAppCtx.JobSharedAppCtxEnum;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobCancelMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobCancelRecoverMsg;
import edu.utexas.tacc.tapis.jobs.utils.DataLocator;
import edu.utexas.tacc.tapis.jobs.utils.JobOutputInfo;
import edu.utexas.tacc.tapis.jobs.utils.SelectTuple;
import edu.utexas.tacc.tapis.notifications.client.NotificationsClient;
import edu.utexas.tacc.tapis.notifications.client.gen.model.ReqPostSubscription;
import edu.utexas.tacc.tapis.notifications.client.gen.model.RespSubscriptions;
import edu.utexas.tacc.tapis.notifications.client.gen.model.TapisSubscription;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqShareResource;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShare;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShareList;
import edu.utexas.tacc.tapis.security.client.model.SKShareDeleteShareParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareGetSharesParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareHasPrivilegeParms;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;


public final class JobsImpl 
 extends BaseImpl
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobsImpl.class);
    public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singleton instance of this class.
    private static JobsImpl _instance;
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private JobsImpl() {}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    public static JobsImpl getInstance()
    {
        // Create the singleton instance if necessary.
        if (_instance == null) {
            synchronized (JobsImpl.class) {
                if (_instance == null) _instance = new JobsImpl();
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobListByUsername:                                                  */
    /* ---------------------------------------------------------------------- */
    public List<JobListDTO> getJobListByUsername(String user, String tenant, List<OrderBy> orderByList, Integer limit,Integer skip) 
     throws TapisImplException
    {
        // ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobListByUsername", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobListByUsername", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // ----- Get the job list.
        List<JobListDTO> jobList = new ArrayList<JobListDTO>();
        try {jobList =  getJobsDao().getJobsByUsername(user, tenant, orderByList,limit, skip);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SELECT_BY_USERNAME_ERROR", user, tenant,e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
      
        
        // Could be null if not found.
        return jobList;
    }
    
    
    /* ---------------------------------------------------------------------- */
    /* getJobListCountByUsername:                                             */
    /* ---------------------------------------------------------------------- */
   
    public int getJobsSearchListCountByUsername(String user, String tenant, List<String> searchList,
    		List<OrderBy> orderByList, boolean sharedWithMe) 
    		throws TapisImplException
    {
    	// ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListCountByUsername", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListCountByUsername", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
     
     

      // Build verified list of search conditions
      var verifiedSearchList = new ArrayList<String>();
      if (searchList != null && !searchList.isEmpty())
      {
        try
        {
          for (String cond : searchList)
          {
            // Use SearchUtils to validate condition
            String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
            verifiedSearchList.add(verifiedCondStr);
          }
        }
        catch (Exception e)
        {        
           String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", "", user, tenant, e);
           _log.error(msg, e);
           throw new IllegalArgumentException(msg);
              
        }
      }

      int count= 0;

      // Count all allowed jobs matching the search conditions
      try {
		count =  getJobsDao().getJobsSearchListCountByUsername(user, tenant, verifiedSearchList, orderByList, sharedWithMe) ;
	} catch (TapisException e) {
		String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_COUNT_ERROR", user, tenant);
        throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
		}
      return count;
    }

    /* ---------------------------------------------------------------------- */
    /* getJobsSearchListCountByUsernameUsingSqlSearchStr:                     */
    /* ---------------------------------------------------------------------- */
   
    public int getJobsSearchListCountByUsernameUsingSqlSearchStr(String user, String tenant, 
    		String sqlSearchStr, List<OrderBy> orderByList, boolean sharedWithMe) 
    		throws TapisImplException
    {
    	// ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobsSearchListCountByUsernameUsingSqlSearchStr", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobsSearchListCountByUsernameUsingSqlSearchStr", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
     
        ASTNode searchAST;
        try { 
        	searchAST = ASTParser.parse(sqlSearchStr); 
        } catch (Exception e){
           String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", sqlSearchStr, user, tenant, e);
           _log.error(msg, e);
           throw new IllegalArgumentException(msg);
        }

       int count= 0;

       // Count all allowed jobs matching the search conditions
       try {
		 count =  getJobsDao().getJobsSearchListCountByUsernameUsingSqlSearchStr(user, tenant, searchAST, orderByList,sharedWithMe) ;
	   } catch (TapisException e) {
		 String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_COUNT_ERROR", user, tenant);
         throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
	   }
       return count;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobSearchListByUsername:                                            */
    /* ---------------------------------------------------------------------- */
    public List<JobListDTO> getJobSearchListByUsername(String user, String tenant, List<String>searchList, 
    		List<OrderBy> orderByList, Integer limit,Integer skip, boolean sharedWithMe) 
     throws TapisImplException
    {
        // ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // Build verified list of search conditions
        var verifiedSearchList = new ArrayList<String>();
        if (searchList != null && !searchList.isEmpty())
        {
          try
          {
            for (String cond : searchList)
            {
              // Use SearchUtils to validate condition
              String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
              verifiedSearchList.add(verifiedCondStr);
            }
          }
          catch (Exception e)
          {
            String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", "", user, tenant, e);;
            _log.error(msg, e);
            throw new IllegalArgumentException(msg);
          }
        }
        // ----- Get the job list.
        List<JobListDTO> jobList = null;
        try {jobList = getJobsDao().getJobsSearchByUsername(user, tenant, verifiedSearchList, orderByList, limit,
        		skip, sharedWithMe);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_ERROR", user, tenant, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        // Could be null if not found.
        return jobList;
    }

    /* ---------------------------------------------------------------------- */
    /* getJobSearchListByUsernameUsingSqlSearchStr:                           */
    /* ---------------------------------------------------------------------- */
    public List<JobListDTO> getJobSearchListByUsernameUsingSqlSearchStr(String user, String tenant, 
    		String sqlSearchStr, List<OrderBy> orderByList, Integer limit,Integer skip,boolean sharedWithMe) 
     throws TapisImplException
    {
        // ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        if (StringUtils.isBlank(sqlSearchStr)) {
        	return getJobSearchListByUsername(user, tenant, null,  orderByList, limit, skip,false) ;
        } 
        
        ASTNode searchAST;
        try { searchAST = ASTParser.parse(sqlSearchStr); }
        catch (Exception e)
        {
          String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", sqlSearchStr, user, tenant, e);
          _log.error(msg, e);
          throw new IllegalArgumentException(msg);
        }
       
        // ----- Get the job list.
        List<JobListDTO> jobList = null;
        try {jobList = getJobsDao().getJobSearchListByUsernameUsingSqlSearchStr(user, tenant, searchAST,
        		orderByList, limit, skip,sharedWithMe);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_ERROR", user, tenant, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        // Could be null if not found.
        return jobList;
    }
    
    
    /* ---------------------------------------------------------------------- */
    /* getJobSearchAllAttributesByUsername:                                   */
    /* ---------------------------------------------------------------------- */
    public List<Job>  getJobSearchAllAttributesByUsername(String user, String tenant, List<String>searchList,
    		List<OrderBy> orderByList, Integer limit,Integer skip, boolean shared) 
     throws TapisImplException
    {
        // ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // Build verified list of search conditions
        var verifiedSearchList = new ArrayList<String>();
        if (searchList != null && !searchList.isEmpty())
        {
          try
          {
            for (String cond : searchList)
            {
              // Use SearchUtils to validate condition
              String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
              verifiedSearchList.add(verifiedCondStr);
            }
          }
          catch (Exception e)
          {
            String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", "", user, tenant, e);
            _log.error(msg, e);
            throw new IllegalArgumentException(msg);
          }
        }
        // ----- Get the job list.
        List<Job> jobList = null;
       
        try {jobList = getJobsDao().getJobSearchAllAttributesByUsername(user, tenant, verifiedSearchList,orderByList,limit,skip,shared);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_ERROR", user, tenant, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        // Could be null if not found.
        return jobList;
    }
     
    /* ---------------------------------------------------------------------- */
    /* getJobKeyList:                                                         */
    /* ---------------------------------------------------------------------- */
    /*
     * Get the jobs table field name
     */
    public Set<String> getJobKeyList(){
    	Set<String> jobKeySet = JobsDao.JOB_REQ_DB_MAP.keySet();
    	return jobKeySet;
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkSelectListValidity:                                               */
    /* ---------------------------------------------------------------------- */
    /**
     * Check the select list validity specified in the search end-point
     * @param selectList
     * @return
     */
    public SelectTuple checkSelectListValidity(List<String> selectList) {
    	
    	SelectTuple inValidTuple = new SelectTuple(true,"str");;
    	Set<String>jobKeyList = getJobKeyList();
    	//_log.debug("getJobKeyList: " +jobKeyList.toString() );
    	
    	for(String key : selectList) {
    		//_log.debug("contains: " + jobKeyList.contains(SearchUtils.camelCaseToSnakeCase(key)));
    		if (!jobKeyList.contains(SearchUtils.camelCaseToSnakeCase(key)) && !key.equals("allAttributes") && !key.equals("summaryAttributes")) {
    			//_log.debug("select camel case to snake case key:"+ SearchUtils.camelCaseToSnakeCase(key));
    			inValidTuple = new SelectTuple(false,key);
    			break;
    		}
    		
    	}
    	return inValidTuple;
    }
    /* ---------------------------------------------------------------------- */
    /* getJobSearchAllAttributesByUsernameUsingSqlSearchStr:                  */
    /* ---------------------------------------------------------------------- */
    public List<Job>  getJobSearchAllAttributesByUsernameUsingSqlSearchStr(String user, String tenant,
    		String sqlSearchStr , List<OrderBy> orderByList, Integer limit,Integer skip, boolean sharedWithMe) 
     throws TapisImplException
    {
        // ----- Check input.
        
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobSearchListByUsername", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        ASTNode searchAST;
        try { searchAST = ASTParser.parse(sqlSearchStr); }
        catch (Exception e)
        {
          String msg =  MsgUtils.getMsg("JOBS_SEARCH_ERROR", sqlSearchStr, user, tenant, e);
          _log.error(msg, e);
          throw new IllegalArgumentException(msg);
        }
     
        // ----- Get the job list.
        List<Job> jobList = null;
        try {jobList = getJobsDao().getJobSearchAllAttributesByUsernameUsingSqlSearchStr(user, tenant, searchAST,orderByList,limit,skip, sharedWithMe);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_ERROR", user, tenant, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        // Could be null if not found.
        return jobList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobByUuid:                                                          */
    /* ---------------------------------------------------------------------- */
    public Job getJobByUuid(String jobUuid, String user, String tenant) 
     throws TapisImplException
    {  
    	
        // ----- Check input.
        if (StringUtils.isBlank(jobUuid)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobByUuid", "jobUuid");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobByUuid", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobByUuid", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // ----- Get the job.
        Job job = null;
        try {job = getJobsDao().getJobByUUID(jobUuid, true);}
        catch (TapisNotFoundException e) {
            String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", jobUuid, user, tenant, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", jobUuid, user, tenant, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        
        // ----- Authorization checks.
        // Make sure the user and tenant are authorized.
        if (!tenant.equals(job.getTenant())) 
        {
	        String msg = MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", tenant, job.getTenant());
	        throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	    }
        
        /**
         * Check if the user is the job owner, or the tenant admin or the one who created the job
         * If none of the above is true, then the user is not authorized
         * */
        
        if (!user.equals(job.getOwner()) && 
        	!user.equals(job.getCreatedby()) && 
        	!isAdminSafe(user, tenant)) 
        {
            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, job.getOwner());
            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
        }
       
        // Could be null if not found.
        return job;
    }
    
   
    /* ---------------------------------------------------------------------- */
    /* getJobByUuid:                                                          */
    /* ---------------------------------------------------------------------- */
    public Job getJobByUuid(String jobUuid, String user, String tenant, 
                            String jobResourceShareType, String privilege) 
     throws TapisImplException
    {  
    	
    	boolean checkShare = false;
    	Job job = null;
    	String jobOwner = "";
        try {job = getJobByUuid(jobUuid, user, tenant);}
        catch (Exception e) {
        	   if (!e.getMessage().startsWith("JOBS_MISMATCHED_OWNER")) 
        		   throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);
        	   // get jobowner information from the exception message
        	   String[] parseMsg = e.getMessage().split("\\(");
        	   jobOwner = parseMsg[2].substring(0,parseMsg[2].length()-2);
        	   _log.debug("Parsed jobOwner from the message = " + jobOwner);
               checkShare = true;
               
            }
        
        // ----- Share Authorization checks.
       if (checkShare) {   
	        /**
	         * 
	         * If the user is not the job owner or not admin or not the one who created the job,
	         * we need to check if the job has been shared with the user.
	         * */
    	    if (!isJobShared(jobUuid, user, tenant, jobResourceShareType, privilege)) {
    	        String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, jobOwner);
       		     _log.error(msg);
       		     throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
    	    
    	    try {job = getJobsDao().getJobByUUID(jobUuid, true);}
            catch (TapisNotFoundException e) {
                String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", jobUuid, user, tenant, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", jobUuid, user, tenant, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
            
    	    
	    }
        
        // Could be null if not found.
        return job;
    }
   
    /* ---------------------------------------------------------------------- */
    /* getJobStatusByUuid:                                                    */
    /* ---------------------------------------------------------------------- */
    public JobStatusDTO getJobStatusByUuid(String jobUuid, String user, String tenant) 
     throws TapisImplException
    {  
    
        // ----- Check input.
        if (StringUtils.isBlank(jobUuid)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobByUuid", "jobUuid");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobByUuid", "user");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobByUuid", "tenant");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // ----- Get the job status, job owner, createdby, createdby tenant and visible information
        JobStatusDTO jobstatus = null;
        try {jobstatus = getJobsDao().getJobStatusByUUID(jobUuid);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", jobUuid, user, tenant,e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        
        // ----- Authorization checks.
        // Make sure the user and tenant are authorized.
       if(jobstatus != null) {
	        if (!tenant.equals(jobstatus.getTenant())) {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", tenant, jobstatus.getTenant());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
	        
	        /**
	         * Check if the user is the job owner, or the tenant admin or the one who created the job
	         * If none of the above is true, then the user is not authorized
	         * */
	        
	        if (!user.equals(jobstatus.getOwner()) && 
	        	!user.equals(jobstatus.getCreatedBy()) && 
	        	!isAdminSafe(user, tenant))
	        	
	        {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, jobstatus.getOwner());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
	        
	      }
        
        // Could be null if not found.
        return jobstatus;
    }

    /* ---------------------------------------------------------------------- */
    /* getJobStatusByUuid:                                                    */
    /* ---------------------------------------------------------------------- */
    public JobStatusDTO getJobStatusByUuid(String jobUuid, String user, 
                                           String tenant, String jobResourceShareType, 
                                           String privilege) 
     throws TapisImplException
    {  
        // ----- Get the job status, job owner, createdby, createdby tenant and visible information
        JobStatusDTO jobstatus = null;
        boolean checkShare = false;
        String jobOwner = "";
        try {jobstatus = getJobStatusByUuid(jobUuid, user, tenant);}
        catch (Exception e) {
        	   if (!e.getMessage().startsWith("JOBS_MISMATCHED_OWNER")) 
        		   throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);
        	   // get jobowner information from the exception message
        	   String[] parseMsg = e.getMessage().split("\\(");
        	   jobOwner = parseMsg[2].substring(0,parseMsg[2].length()-2);
        	   _log.debug("Parsed jobOwner from the message = " + jobOwner);
               checkShare = true;
         }
           
        // ----- Share Authorization checks.
        if(checkShare) {    
	        /**
	         * If the user is not the job owner or not admin or not the one who created the job, we need to check if the job has been shared with the user.
	         * */
    	    if(!isJobShared(jobUuid, user, tenant, jobResourceShareType, privilege)) {
	       		String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, jobOwner);
	       		 _log.error(msg);
	       		throw new TapisImplException(msg, Condition.UNAUTHORIZED);
            }
    	    try {jobstatus = getJobsDao().getJobStatusByUUID(jobUuid);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", jobUuid, user, tenant,e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
	    }
        
        // Could be null if not found.
        return jobstatus;
    }
    
    /* ---------------------------------------------------------------------- */
    /* isJobShared:                                                           */
    /* ---------------------------------------------------------------------- */
    public boolean isJobShared(String jobUuid, String user, String tenant, 
    		String jobResourceShareType, String privilege ) 
    		throws TapisImplException 
    {
    	 
    	boolean shareFlag = false;
    	SKClient skClient = getSKClient();
    	
         
         SKShareHasPrivilegeParms skParams = new SKShareHasPrivilegeParms();
         skParams.setGrantee(user);
         skParams.setResourceId1(jobUuid);
         skParams.setResourceType(jobResourceShareType);
         skParams.setPrivilege(privilege);
         skParams.setTenant(tenant);
         
         try {
				shareFlag = skClient.hasPrivilege(skParams);
			} catch (TapisClientException e) {
				String msg = MsgUtils.getMsg("JOBS_SHARE_NO_PRIVILEDGE_ERROR", jobUuid, tenant, user, e.getMessage());
				throw new TapisImplException(msg, e, Condition.UNAUTHORIZED);
			}
		return shareFlag;
    }

   
    
    /* ---------------------------------------------------------------------- */
    /* getJobOutputList:                                                      */
    /* ---------------------------------------------------------------------- */
    public List<FileInfo> getJobOutputList(Job job, String tenant, String user, 
                                           String pathName, int limit, int skip, 
    		                               String jobResourceShareType, String privilege) 
     throws TapisImplException
    {
        // ----- Get the job output files list.
        DataLocator dataLocator = new DataLocator(job);
        
        JobOutputInfo jobOutputFilesinfo = dataLocator.getJobOutputSystemInfo(pathName);
        
        // We checked if the job is shared within the method getJobByUuid(). We never return the value of the check. 
        // So, we need to check it again here if the job is shared and accordingly set the impersonationId
        boolean isSharedAppCtx = checkSharedAppCtx(job, jobOutputFilesinfo);
        
        String sharedAppCtx = "";
        if (isSharedAppCtx == true) {
        	sharedAppCtx = job.getSharedAppCtx();
        }
        
        boolean skipTapisAuthorization = isJobShared(job.getUuid(), user, tenant, jobResourceShareType, privilege) || isSharedAppCtx;
        String impersonationId =  null;
        if(skipTapisAuthorization == true) {
        	impersonationId = job.getOwner();
        }
        List<FileInfo> outputList = dataLocator.getJobOutputListings(
                jobOutputFilesinfo, tenant, user, limit, skip, impersonationId, sharedAppCtx);
               
        return outputList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkSharedAppCtx:                                                     */
    /* ---------------------------------------------------------------------- */
    /**
     * 
     * @param job
     * @param jobOutputFilesinfo
     * @return true if job has shared app context and exec or archive system path are shared
     */
    public boolean checkSharedAppCtx(Job job, JobOutputInfo jobOutputFilesinfo) {
    	
    	boolean isSharedAppPathAllowed = false;
    	
    	// check if job ran on shared app context.
    	if (StringUtils.isBlank(job.getSharedAppCtx()))
    		return isSharedAppPathAllowed;
    	
    	List<JobSharedAppCtxEnum> sharedAppCtxAttribs = job.getSharedAppCtxAttribs();
    	
    	// check if job output files are in the archive system
    	if (jobOutputFilesinfo.isArchiveSystem()){
    		// check if archive system and archive dir path are shared in the shared app context.
    		if(sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID) 
    		   && sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_DIR)){
    			  isSharedAppPathAllowed = true;
    		}
    	} else {
    		// check if exec system and exec system dir path are shared in the shared app context.
    		// note we are not checking input dir as we are interested only in job output files
    		if(sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_ID) 
    		   && sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_OUTPUT_DIR)) {
    			 isSharedAppPathAllowed = true;
    		}
    	}	
    	
    	return isSharedAppPathAllowed;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobOutputDownloadInfo:                                              */
    /* ---------------------------------------------------------------------- */
    public JobOutputInfo  getJobOutputDownloadInfo(Job job, String tenant, String user, String pathName) 
     throws TapisImplException
    {
        // ----- Download Info for the job output files.
        DataLocator dataLocator = new DataLocator(job);
        
        JobOutputInfo jobOutputFilesinfo = dataLocator.getJobOutputSystemInfo(pathName);
       
        return jobOutputFilesinfo;   
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobEventByUuid:                                                     */
    /* ---------------------------------------------------------------------- */
    public List<JobEvent> getJobEventsByJobUuid(String jobUuid, String user, String tenant, int limit, int skip) 
     throws TapisImplException
    {
        // ----- Check input.
        if (StringUtils.isBlank(jobUuid)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobEventByUuid", "jobUuid");
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // ----- Get the job events.
        List<JobEvent> jobEvents = null;
        try {
        	jobEvents = getJobEventsDao().getJobEventsByJobUUID(jobUuid, limit, skip);
        }
        catch (TapisNotFoundException e) {
            String msg = MsgUtils.getMsg("JOBS_JOBEVENT_SELECT_UUID_ERROR", tenant, user, jobUuid, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOBEVENT_SELECT_UUID_ERROR", tenant, user, jobUuid, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
              
        // Could be null if not found.
        return jobEvents;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobEventsSummary:                                                   */
    /* ---------------------------------------------------------------------- */
    public List<JobHistoryDisplayDTO> getJobEventsSummary(List<JobEvent> jobEvents, String user, 
                                                          String tenant, JobStatusDTO jobstatus) 
     throws TapisImplException
    {   

        
        boolean skipTapisAuthorization = isJobShared(jobstatus.getJobUuid(), user, tenant, 
     		   JobResourceShare.JOB_HISTORY.name(), JobTapisPermission.READ.name());
        String impersonationId =  null;
        if(skipTapisAuthorization == true) {
        	impersonationId = jobstatus.getOwner();
        }
    	
		ArrayList<JobHistoryDisplayDTO> eventsSummary = new ArrayList<JobHistoryDisplayDTO>();
        for(JobEvent jobEvent: jobEvents ) {
        	JobHistoryDisplayDTO historyObj = new JobHistoryDisplayDTO(jobEvent, user, tenant, impersonationId);
        	eventsSummary.add(historyObj);
        }
              
        // Could be null if not found.
        return eventsSummary;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* doCancelJob:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Perform the cancellation operation and return whether we succeeded or not.
     * 
     * @param jobUuid the job to be cancelled
     * @param prettyPrint whether to pretty print the response
     * @param threadContext the previously retrieved thread context
     * @return true for success or false if an error was encountered 
     */
    public boolean doCancelJob(String jobUuid, TapisThreadContext threadContext)
      {
        // Initialized to success.
        boolean result = true;
        
        try {
            // get a JobQueueManager instance and prep a JobCancelMsg
            JobQueueManager queueManager = JobQueueManager.getInstance();
          
            // set correlation id and sender
            JobCancelMsg jobCancelMsg = new JobCancelMsg();
            jobCancelMsg.jobuuid = jobUuid;
            jobCancelMsg.correlationId = jobUuid;
            jobCancelMsg.senderId = this.getClass().getSimpleName() + "-cancelCmdStatus";
          
            // post a cmd to our job to cancel
            queueManager.postCmdToJob(jobCancelMsg, jobUuid);
            
            //TODO How do we know if a job is in recovery queue?
            JobCancelRecoverMsg jobCancelRecoverMsg = new JobCancelRecoverMsg();
            jobCancelRecoverMsg.jobUuid = jobUuid;
            jobCancelRecoverMsg.tenantId = threadContext.getOboTenantId();
            jobCancelRecoverMsg.setSenderId(this.getClass().getSimpleName() + "-cancelCmdStatus");
        
            // post a cmd to a job that is in recovery
            queueManager.postRecoveryQueue(jobCancelRecoverMsg);

          } catch (JobException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_POST_CANCEL", jobUuid);
            _log.error(msg, e);
            result = false; // failure 
          }
          
        // Return result code.
        return result;
    }
    
    /* ---------------------------------------------------------------------- */
    /* doHideJob:                                                             */
    /* ---------------------------------------------------------------------- */
    public boolean doHideJob(String jobUuid, String tenant, String user) 
    {
        try { 
        	getJobsDao().setJobVisibility(jobUuid, tenant, user,false);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOB_VISIBILITY_UPDATE_ERROR", jobUuid, user, tenant,e);
            _log.error(msg, e);
           
        }
        
        // Could be null if not found.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* doUnHideJob:                                                           */
    /* ---------------------------------------------------------------------- */
    public boolean doUnHideJob(String jobUuid, String tenant, String user) 
    {
        try { 
        	getJobsDao().setJobVisibility(jobUuid, tenant, user,true);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOB_VISIBILITY_UPDATE_ERROR", jobUuid, user,
            		tenant,e);
            _log.error(msg, e);
           
        }
        
        // Could be null if not found.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createShareJob:                                                        */
    /* ---------------------------------------------------------------------- */
    public void createShareJob(JobShared jobShared) throws TapisException 
    {
    	 SKClient skClient = getSKClient();
    	 
         validateNewSharedJob(jobShared);
         
         ReqShareResource resourceShared = new ReqShareResource();
         resourceShared.setGrantee(jobShared.getGrantee());
         resourceShared.setResourceType(jobShared.getJobResource().name());
         resourceShared.setResourceId1(jobShared.getJobUuid());
         resourceShared.setResourceId2(null);
         resourceShared.setPrivilege(jobShared.getJobPermission().name());
         resourceShared.setGrantor(jobShared.getGrantor());
         resourceShared.setTenant(jobShared.getTenant());
         
         
         String url = "";
         
         try {
			url = skClient.shareResource(resourceShared);
			_log.debug("Resource is sucessfully shared: " + url);
		 } catch (TapisClientException e) {
			 String msg = MsgUtils.getMsg("JOBS_JOB_SHARE_SK_CLIENT_INSERT_ERROR", jobShared.getJobUuid(), jobShared.getCreatedby(),
	            		jobShared.getJobResource(),e);
	         _log.error(msg, e);
	         throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
		 }
    }
   
    /* ---------------------------------------------------------------------- */
    /* deleteShareJob:                                                        */
    /* ---------------------------------------------------------------------- */
    public void deleteShareJob(JobShared js, String tenant, String grantor) throws TapisException 
    
    {
    	 SKClient skClient = getSKClient();
        
        
         SKShareDeleteShareParms param = new SKShareDeleteShareParms();
         param.setGrantee(js.getGrantee());
         param.setResourceId1(js.getJobUuid());
         param.setResourceType(js.getJobResource().name());
         param.setPrivilege(JobTapisPermission.READ.name());
         param.setTenant(tenant);
         
         try {
			int i = skClient.deleteShare(param);
			_log.debug("Resource share is sucessfully revoked: " + i);
		 } catch (TapisClientException e) {
			 String msg = MsgUtils.getMsg("JOBS_JOB_SHARE_SK_CLIENT_DELETE_ERROR", js.getJobUuid(), js.getGrantee(),
					 grantor, tenant,e);
	         _log.error(msg, e);
	         throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
		 }
    }
    
    /* ---------------------------------------------------------------------- */
    /* createShareEvent:                                                      */
    /* ---------------------------------------------------------------------- */
    public void createShareEvent(JobShared jshare) throws TapisException 
    
    {
    
    	var eventMgr = JobEventManager.getInstance();
	   
	    try {
	     
	      eventMgr.recordShareEvent(jshare.getJobUuid(), jshare.getTenant(), 
	               jshare.getJobResource().name(), 
	               "SHARE_"+ jshare.getJobResource().name() + "_" + jshare.getJobPermission().name(), 
	               jshare.getGrantee() , jshare.getCreatedby());
	      
	    } catch (TapisNotFoundException e) {
            String msg = MsgUtils.getMsg("JOBS_JOBEVENT_SHARE_EVENT_CREATE_ERROR", jshare.getJobUuid(), jshare.getTenant(),e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOBEVENT_SELECT_UUID_ERROR", jshare.getTenant(), jshare.getCreatedby(), jshare.getJobUuid(), e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* createUnShareEvent:                                                      */
    /* ---------------------------------------------------------------------- */
    public void createUnShareEvent(JobShared js) throws TapisException 
    
    {
    
    	var eventMgr = JobEventManager.getInstance();
	   
	    try {
	      String event = "UNSHARE_"+ js.getJobResource().name() + "_" + js.getJobPermission().name();
	      eventMgr.recordUnShareEvent(js,event);
	      
	    } catch (TapisNotFoundException e) {
            String msg = MsgUtils.getMsg("JOBS_JOBEVENT_UNSHARE_EVENT_CREATE_ERROR", js.getJobUuid(),
            		js.getTenant(), e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_JOBEVENT_SELECT_UUID_ERROR", js.getTenant(), js.getCreatedby(), 
            		js.getGrantee(), js.getJobResource(), e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
    }
    /* ---------------------------------------------------------------------- */
    /* getShareJob:                                                           */
    /* ---------------------------------------------------------------------- */
    public List<JobShareListDTO> getShareJob(String jobUuid, String grantor, 
                                             String obouser, String tenant) 
     throws TapisImplException 
    
    {
    	 SKClient skClient = getSKClient();
         
         
         SKShareGetSharesParms params = new SKShareGetSharesParms();
         params.setResourceId1(jobUuid);
         params.setTenant(tenant);
         SkShareList skShareList = null;
       
         try {
 			skShareList = skClient.getShares(params);
 		 } catch (TapisClientException e) {
 			 String msg = MsgUtils.getMsg("JOBS_JOB_SHARE_SK_RETRIEVE_ERROR",jobUuid, grantor, tenant, e);
 	         _log.error(msg, e);
 	         throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
 		}
        
         List<JobShareListDTO> jobShareList = new ArrayList<JobShareListDTO>();
         // Convert from SK representation to local representation
         for(SkShare sks: skShareList.getShares()) {
        	 JobShared js = new JobShared();
           	 js.setJobResource(JobResourceShare.valueOf(sks.getResourceType()));
        	 js.setCreated(sks.getCreated().toInstant());
        	 js.setJobUuid(sks.getResourceId1());
        	 js.setCreatedby(sks.getGrantor());
        	
        	 js.setJobPermission(JobTapisPermission.valueOf(sks.getPrivilege()));
        	 js.setTenant(sks.getTenant());
        	 js.setUserSharedWith(sks.getGrantee());
        	 jobShareList.add(new JobShareListDTO(js));
         }
         
       return jobShareList; 
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSharesJob:                                                          */
    /* ---------------------------------------------------------------------- */
    public  List<JobShared> getSharesJob(String grantee, String jobUuid, String obouser, 
    		                             String grantor, String tenant) 
     throws TapisImplException 
    {
    	 SKClient skClient = getSKClient();
         
         SKShareGetSharesParms params = new SKShareGetSharesParms();
         params.setGrantee(grantee);
         params.setGrantor(grantor);
         params.setResourceId1(jobUuid);
         params.setTenant(tenant);      
         SkShareList skShareList = null;
       
         try {
 			skShareList = skClient.getShares(params);
 		 } catch (TapisClientException e) {
 			 String msg = MsgUtils.getMsg("JOBS_JOB_SHARE_SK_RETRIEVE_ERROR" ,jobUuid, grantor, tenant, e);
 	         _log.error(msg, e);
 	         throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
 		}
        
         // Convert from SK representation to local representation
         List<JobShared> jobShareList = new ArrayList<JobShared>();
         for(SkShare sks: skShareList.getShares()) {
        	 JobShared js = new JobShared();
           	 js.setJobResource(JobResourceShare.valueOf(sks.getResourceType()));
        	 js.setCreated(sks.getCreated().toInstant());
        	 js.setJobUuid(sks.getResourceId1());
        	 js.setCreatedby(sks.getGrantor());
        	 js.setJobPermission(JobTapisPermission.valueOf(sks.getPrivilege()));
        	 js.setTenant(sks.getTenant());
        	 js.setUserSharedWith(sks.getGrantee());
        	 js.setId(sks.getId());
        	 jobShareList.add(js);
         }
         
       return jobShareList; 
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSharesJob:                                                          */
    /* ---------------------------------------------------------------------- */
    public  List<JobShared> getSharesJob(String user,String tenant) throws TapisImplException 
    
    {
    	 SKClient skClient = getSKClient();
    	 List<JobShared> jobShareList = new ArrayList<JobShared>();
      
         for(JobResourceShare resourceType:JobResourceShare.values()) {
	         SKShareGetSharesParms params = new SKShareGetSharesParms();
	         params.setGrantee(user);
	         params.setTenant(tenant);
	         params.setResourceType(resourceType.name());
         
        
	         SkShareList skShareList = null;
       
	         try {
	 			skShareList = skClient.getShares(params);
	 		 } catch (TapisClientException e) {
	 			 String msg = MsgUtils.getMsg("JOBS_SHARE_SK_RETRIEVE_ERROR" , user, tenant, e);
	 	         _log.error(msg, e);
	 	         throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
	 		}
         
	         // Convert from SK representation to local representation
	        
	         _log.debug("skShareList: "+ skShareList.getShares().toString());
	         for(SkShare sks: skShareList.getShares()) {
	        	 JobShared js = new JobShared();
	           	 js.setJobResource(JobResourceShare.valueOf(sks.getResourceType()));
	        	 js.setCreated(sks.getCreated().toInstant());
	        	 js.setJobUuid(sks.getResourceId1());
	        	 js.setCreatedby(sks.getGrantor());
	        	 js.setJobPermission(JobTapisPermission.valueOf(sks.getPrivilege()));
	        	 js.setTenant(sks.getTenant());
	        	 js.setUserSharedWith(sks.getGrantee());
	        	 js.setId(sks.getId());
	        	 jobShareList.add(js);
	         }
         }
       return jobShareList; 
    }
    
    /* ---------------------------------------------------------------------- */
    /* queryDB:                                                               */
    /* ---------------------------------------------------------------------- */
    /** This monitoring method does very little logging to avoid log thrashing.
     * 
     * @param tableName the table to be queried
     * @return 0 or 1 on success
     * @throws TapisImplException on error
     */
    public int queryDB(String tableName) throws TapisImplException
    {
        // Get the dao.
        JobsDao dao = null;
        try {dao = getJobsDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "query test");
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);         
             }
        
        // Access the table.
        int rows = 0;
        try {rows = dao.queryDB(tableName);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("DB_QUERY_DB_ERROR", tableName);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);         
         }
	        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* ensureDefaultQueueIsDefined:                                           */
    /* ---------------------------------------------------------------------- */
    public void ensureDefaultQueueIsDefined() throws TapisException
    {
        // Is the default queue already defined?
        JobQueue queue = null;
        JobQueuesDao queueDao;
        try {
            // Get the list of all queues in descending priority order.
            queueDao = new JobQueuesDao();
            queue = queueDao.getJobQueueByName(JobQueueManagerNames.getDefaultQueue());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QUEUE_FAILED_QUERY", 
                                         JobQueueManagerNames.getDefaultQueue(), 
                                         e.getMessage());
            throw new JobException(msg, e);
        }
        
        // Is the default queue already defined?
        if (queue != null) return;
        
        // Define the default queue here.
        queue = new JobQueue();
        queue.setName(JobQueueManagerNames.getDefaultQueue());
        queue.setFilter(JobQueueManagerNames.DEFAULT_QUEUE_FILTER);
        queue.setPriority(JobQueuesDao.DEFAULT_TENANT_QUEUE_PRIORITY);
        
        // Create the queue.
        try {queueDao.createQueue(queue);}
        catch (Exception e) {
            if (e.getMessage().startsWith("JOBS_JOB_QUEUE_CREATE_ERROR")) throw e;
            String msg = MsgUtils.getMsg("JOBS_JOB_QUEUE_CREATE_ERROR", 
                                         queue.getName(), e.getMessage());
            throw new JobException(msg, e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* postSubscription:                                                      */
    /* ---------------------------------------------------------------------- */
    public String postSubscription(ReqPostSubscription postReq, String user, String tenant) 
      throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the notification client for this user, which may be cached.
        NotificationsClient notifClient = 
            ServiceClients.getInstance().getClient(user, tenant, NotificationsClient.class);
      
        // Make the call to Notifications.
        return notifClient.postSubscription(postReq);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSubscriptions:                                                      */
    /* ---------------------------------------------------------------------- */
    public RespSubscriptions getSubscriptions(String jobUuid, int limit, int skip,
                                              String user, String tenant) 
     throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the notification client for this user, which may be cached.
        NotificationsClient notifClient = 
            ServiceClients.getInstance().getClient(user, tenant, NotificationsClient.class);
        
        // Get the subscription list and metadata.
        final String orderBy = null;
        final String startsAfter = null;        
        return notifClient.getSubscriptionsBySubjectForAllOwners(jobUuid, limit, orderBy, skip, startsAfter);
    }

    /* ---------------------------------------------------------------------- */
    /* getSubscriptionByUUID:                                                 */
    /* ---------------------------------------------------------------------- */
    public TapisSubscription getSubscriptionByUUID(String uuid, String user, String tenant) 
     throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the notification client for this user, which may be cached.
        NotificationsClient notifClient = 
            ServiceClients.getInstance().getClient(user, tenant, NotificationsClient.class);
        
        // Delete all subscriptions on a job.
        return notifClient.getSubscriptionByUuid(uuid);
    }

    /* ---------------------------------------------------------------------- */
    /* deleteJobSubscriptions:                                                */
    /* ---------------------------------------------------------------------- */
    public int deleteJobSubscriptions(String jobUuid, String user, String tenant) 
     throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the notification client for this user, which may be cached.
        NotificationsClient notifClient = 
            ServiceClients.getInstance().getClient(user, tenant, NotificationsClient.class);
        
        // Delete all subscriptions on a job.
        return notifClient.deleteSubscriptionsBySubjectForAllOwners(jobUuid);
    }

    /* ---------------------------------------------------------------------- */
    /* deleteJobSubscription:                                                 */
    /* ---------------------------------------------------------------------- */
    public int deleteJobSubscription(String uuid, String user, String tenant) 
     throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the notification client for this user, which may be cached.
        NotificationsClient notifClient = 
            ServiceClients.getInstance().getClient(user, tenant, NotificationsClient.class);
        
        // Delete a specific subscription.
        return notifClient.deleteSubscriptionByUUID(uuid);
    }
    
    
    /* ---------------------------------------------------------------------- */
    /* getSKClient:                                                           */
    /* ---------------------------------------------------------------------- */
   
     SKClient getSKClient() throws TapisImplException{
    	SKClient skClient = null;
	   	 TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
	   	 String siteId = threadContext.getSiteId();
	   	 String svcTenant = TenantManager.getInstance().getSiteAdminTenantId(siteId);
	        try {
	            skClient = getServiceClient(SKClient.class, TapisConstants.JOBS_SERVICE, svcTenant);
	        } catch (Exception e) {
	            String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "SK", "getClient", svcTenant, TapisConstants.JOBS_SERVICE);
	            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
	        };
        
    	return skClient;
    }

    /* -----------------------------------------------------------------------------*/
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
   
    /* ********************************************************************** */
	/*                             Private Methods                            */
	/* ********************************************************************** */
    private  void validateNewSharedJob(JobShared jobShared) throws TapisException
    {
     
  	   if (StringUtils.isBlank(jobShared.getJobUuid())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSharedJob", "jobUuid");
	          throw new JobException(msg);
	      }
	      if (StringUtils.isBlank(jobShared.getTenant())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSharedJob", "tenant");
	          throw new JobException(msg);
	      }
	      if (StringUtils.isBlank(jobShared.getCreatedby())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSharedJob", "createdBy");
	          throw new JobException(msg);
	      }
	      if (StringUtils.isBlank(jobShared.getGrantee())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSharedJob", "userSharedWith");
	          throw new JobException(msg);
	      }
	     
	      if (jobShared.getJobResource() == null) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSharedJob", "jobResource");
	          throw new JobException(msg);
	      }
	      
	      if (jobShared.getJobPermission()== null) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSharedJob", "jobPermission");
	          throw new JobException(msg);
	      }
    }
}

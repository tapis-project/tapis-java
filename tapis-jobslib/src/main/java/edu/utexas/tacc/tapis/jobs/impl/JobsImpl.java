package edu.utexas.tacc.tapis.jobs.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.jobs.dao.JobQueuesDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.JobQueue;
import edu.utexas.tacc.tapis.jobs.model.dto.JobHistoryDisplayDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobCancelMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobCancelRecoverMsg;
import edu.utexas.tacc.tapis.jobs.utils.DataLocator;
import edu.utexas.tacc.tapis.jobs.utils.JobOutputInfo;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;


public final class JobsImpl 
 extends BaseImpl
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobsImpl.class);
    
    
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
        List<JobListDTO> jobList = null;
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
   
    public int getJobsSearchListCountByUsername(String user, String tenant, List<String> searchList, List<OrderBy> orderByList) 
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
           String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", user, e.getMessage());
           _log.error(msg, e);
           throw new IllegalArgumentException(msg);
              
        }
      }

      int count= 0;

      // Count all allowed jobs matching the search conditions
      try {
		count =  getJobsDao().getJobsSearchListCountByUsername(user, tenant, verifiedSearchList, orderByList) ;
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
    		String sqlSearchStr, List<OrderBy> orderByList) 
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
           String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", user, e.getMessage());//TODO LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
           _log.error(msg, e);
           throw new IllegalArgumentException(msg);
        }

       int count= 0;

       // Count all allowed jobs matching the search conditions
       try {
		 count =  getJobsDao().getJobsSearchListCountByUsernameUsingSqlSearchStr(user, tenant, searchAST, orderByList) ;
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
    		List<OrderBy> orderByList, Integer limit,Integer skip) 
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
            String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", user, e.getMessage());
            _log.error(msg, e);
            throw new IllegalArgumentException(msg);
          }
        }
        // ----- Get the job list.
        List<JobListDTO> jobList = null;
        try {jobList = getJobsDao().getJobsSearchByUsername(user, tenant, verifiedSearchList,orderByList,limit,skip);}
        catch (Exception e) {
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
    		String sqlSearchStr, List<OrderBy> orderByList, Integer limit,Integer skip) 
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
        	return getJobSearchListByUsername(user, tenant, null,  orderByList, limit, skip) ;
        } 
        
        ASTNode searchAST;
        try { searchAST = ASTParser.parse(sqlSearchStr); }
        catch (Exception e)
        {
          String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", user, e.getMessage());;//TODO LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
          _log.error(msg, e);
          throw new IllegalArgumentException(msg);
        }
       
        // ----- Get the job list.
        List<JobListDTO> jobList = null;
        try {jobList = getJobsDao().getJobSearchListByUsernameUsingSqlSearchStr(user, tenant, searchAST,
        		orderByList, limit, skip);}
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
    		List<OrderBy> orderByList, Integer limit,Integer skip) 
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
            String msg = MsgUtils.getMsg("JOBS_SEARCH_ERROR", user, e.getMessage());
            _log.error(msg, e);
            throw new IllegalArgumentException(msg);
          }
        }
        // ----- Get the job list.
        List<Job> jobList = null;
        try {jobList = getJobsDao().getJobSearchAllAttributesByUsername(user, tenant, verifiedSearchList,orderByList,limit,skip);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SEARCHLIST_ERROR", user, tenant, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        // Could be null if not found.
        return jobList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobSearchAllAttributesByUsernameUsingSqlSearchStr:                  */
    /* ---------------------------------------------------------------------- */
    public List<Job>  getJobSearchAllAttributesByUsernameUsingSqlSearchStr(String user, String tenant,
    		String sqlSearchStr , List<OrderBy> orderByList, Integer limit,Integer skip) 
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
          String msg =  MsgUtils.getMsg("JOBS_SEARCH_ERROR", user, e.getMessage());//TODO LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
          _log.error(msg, e);
          throw new IllegalArgumentException(msg);
        }
     
        // ----- Get the job list.
        List<Job> jobList = null;
        try {jobList = getJobsDao().getJobSearchAllAttributesByUsernameUsingSqlSearchStr(user, tenant, searchAST,orderByList,limit,skip);}
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
        if (!tenant.equals(job.getTenant())) {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", tenant, job.getTenant());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
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
        
        // ----- Get the job status.
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
	        if (!user.equals(jobstatus.getOwner()) && 
	            !user.equals(jobstatus.getCreatedBy()) && 
	            !isAdminSafe(user, tenant) &&
	        	!user.equals(jobstatus.getCreatedByTenant()))
	        {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, jobstatus.getOwner());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
        }
        
        // Could be null if not found.
        return jobstatus;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobOutputList:                                                      */
    /* ---------------------------------------------------------------------- */
    public List<FileInfo> getJobOutputList(Job job, String tenant, String user, String pathName, int limit, int skip) 
     throws TapisImplException
    {
        // ----- Check input.
        if (StringUtils.isBlank(job.getUuid())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobOutputList", "jobUuid");
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
        
        // ----- Authorization checks.
        // Make sure the user and tenant are authorized.
        if (!tenant.equals(job.getTenant())) {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", tenant, job.getTenant());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
	        if (!user.equals(job.getOwner()) && 
	            !user.equals(job.getCreatedby()) && 
	            !isAdminSafe(user, tenant)) 
	        {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, job.getOwner());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
        
        // ----- Get the job output files list.
        DataLocator dataLocator = new DataLocator(job);
        
        JobOutputInfo jobOutputFilesinfo = dataLocator.getJobOutputSystemInfo(pathName);
        
        List<FileInfo> outputList = dataLocator.getJobOutputListings(jobOutputFilesinfo, tenant, user, limit, skip);
               
        return outputList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobOutputDownloadInfo:                                              */
    /* ---------------------------------------------------------------------- */
    public JobOutputInfo  getJobOutputDownloadInfo(Job job, String tenant, String user, String pathName) throws TapisImplException
    {
        // ----- Check input.
        if (StringUtils.isBlank(job.getUuid())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobOutputList", "jobUuid");
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
        
        // ----- Authorization checks.
        // Make sure the user and tenant are authorized.
        if (!tenant.equals(job.getTenant())) {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_TENANT", tenant, job.getTenant());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
	        if (!user.equals(job.getOwner()) && 
	            !user.equals(job.getCreatedby()) && 
	            !isAdminSafe(user, tenant)) 
	        {
	            String msg = MsgUtils.getMsg("JOBS_MISMATCHED_OWNER", user, job.getOwner());
	            throw new TapisImplException(msg, Condition.UNAUTHORIZED);
	        }
        
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
    public List<JobHistoryDisplayDTO> getJobEventsSummary(List<JobEvent> jobEvents, String user, String tenant) 
     throws TapisImplException
    {   
    	
		ArrayList<JobHistoryDisplayDTO> eventsSummary = new ArrayList<JobHistoryDisplayDTO>();
        for(JobEvent jobEvent: jobEvents ) {
        	JobHistoryDisplayDTO historyObj = new JobHistoryDisplayDTO(jobEvent, user, tenant);
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
    
  
}

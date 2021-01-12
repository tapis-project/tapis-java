package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.statemachine.JobFSMUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;

/** A note about querying our JSON data types.  The jobs database schema currently defines these 
 * fields as jsonb:  inputs, parameters, execSystemConstraints and notifications.  See the flyway 
 * scripts in tapis-jobsmigrate for the complete definition.  The SubmitJobRequest.json schema in 
 * tapis-jobsapi defines the json dchema that validates job submission requests.  
 * 
 * The jsonb database type allows for indexed searches of json data.  Initially, only one json 
 * index is defined on the exec_system_constraints field.  All searches of json data that do not 
 * select this index will cause a full table scan if no other index is employed.
 * 
 * Here is the json GIN index defined on the jobs exec_system_constraints field:
 * 
 *  I1: CREATE INDEX jobs_exec_sys_constraints_idx ON jobs USING GIN ((exec_system_constraints));
 * 
 * An example of the type of query to which the index will be applied is:
 * 
 * USE:
 *  Q1: Select * from jobs where exec_system_constraints @@ '$.execSystemConstraints[*].key == "key1"'
 * 
 * This query uses the jsonpath predicate operator, @@, which evaluates an expression that includes
 * a jsonpath.  The above query could have had the ::jsonpath type appended to the end.  
 * 
 * Note that execSystemConstraints is an array of json objects.  The specification of the @@ operator 
 * in the query triggers the use of index I1.  Also note the very particular syntax that must be
 * used to activate the index:  Any of "key", "op" and/or "value" can be included in the path filter 
 * as they are all valid components of constraint objects.
 * 
 * Other json operators such as the containment operator, @>, will not trigger the use of index I1
 * and will result in a full table scan unless there's another where clause that uses an index.
 * 
 * DON'T USE:
 *  Q2: Select * from jobs where where exec_system_constraints -> 'execSystemConstraints' @> '[{"key": "key1"}]'
 * 
 * Alternate Approach (not implemented)
 * ------------------------------------
 * An alternative approach would embed execSystemConstraints in the existing parameters column.  
 * In this case, we would use a json GIN EXPRESSION index defined on the parameters field: 
 * 
 * 	I2:	CREATE INDEX jobs_exec_sys_constraints_idx ON jobs USING GIN ((parameters -> 'execSystemConstraints'));
 * 
 * An example of the type of query to which the index will be applied is:
 * 
 * 	Q3:	Select * from jobs where parameters -> 'execSystemConstraints' @> '[{"key": "key1"}]'::jsonb;
 *
 * The use of the GIN EXPRESSION index rather than a simple index on a whole column has several side 
 * effects.  On the positive side, expression indexes are often smaller and faster.  In the negative side, 
 * indexed searches are limited to the execSystemConstraints document subtree and only certain operators 
 * will trigger indexed searches.  In particular, query Q1 uses index I1 but not I2; Q3 uses I2 but not 
 * whole column indexes like I1. 
 *
 * Expression indexing was not chosen because the queries it requires seem less intuitive than the 
 * jsonpath queries that use full column indexing.
 * 
 * The postgres support for json is extensive but somewhat complicated to get right.
 * See https://www.postgresql.org/docs/12/datatype-json.html
 * 
 * @author rcardone
 */
public final class JobsDao 
 extends AbstractDao
{
    /* ********************************************************************** */
	/*                               Constants                                */
	/* ********************************************************************** */
	// Tracing.
	private static final Logger _log = LoggerFactory.getLogger(JobsDao.class);
	  
    // Keep track of the last monitoring outcome.
	private static final CallSiteToggle _lastQueryDBSucceeded = new CallSiteToggle();
	
	// Message when creating job.
	private static final String JOB_CREATE_MSG = "Job created";
	
	// Length of last message.
	private static final int JOB_LEN_4096 = 4096;
	  
	/* ********************************************************************** */
	/*                              Constructors                              */
	/* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* constructor:                                                           */
	/* ---------------------------------------------------------------------- */
	/** The superclass initializes the datasource.
	 * 
	 * @throws TapisException on database errors
	 */
	public JobsDao() throws TapisException {}
	  
	/* ********************************************************************** */
	/*                             Public Methods                             */
	/* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* getJobs:                                                               */
	/* ---------------------------------------------------------------------- */
	public List<Job> getJobs() 
	  throws JobException
	{
	    // Initialize result.
	    ArrayList<Job> list = new ArrayList<>();
     
	    // ------------------------- Call SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          Job obj = populateJobs(rs);
	          while (obj != null) {
	            list.add(obj);
	            obj = populateJobs(rs);
	          }
	          
	          // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
	          throw new JobException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
	            catch (Exception e) 
	            {
	              // If commit worked, we can swallow the exception.  
	              // If not, the commit exception will be thrown.
	              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
	              _log.error(msg, e);
	            }
	      }
	      
	      return list;
	    }

	/* ---------------------------------------------------------------------- */  
	/* getJobByUUID:                                                          */
	/* ---------------------------------------------------------------------- */
	public Job getJobByUUID(String uuid) 
	  throws JobException
	{
	    // ------------------------- Check Input -------------------------
	    if (StringUtils.isBlank(uuid)) {
	        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobsByUUID", "uuid");
	        throw new JobException(msg);
	    }
	      
	    // Initialize result.
	    Job result = null;

	    // ------------------------- Call SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_BY_UUID;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, uuid);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          result = populateJobs(rs);
	          
	          // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", uuid, e.getMessage());
	          throw new JobException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
	            catch (Exception e) 
	            {
	              // If commit worked, we can swallow the exception.  
	              // If not, the commit exception will be thrown.
	              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
	              _log.error(msg, e);
	            }
	      }
	      
	    return result;
	}

	/* ---------------------------------------------------------------------- */
	/* createJob:                                                             */
	/* ---------------------------------------------------------------------- */
	public void createJob(Job job)
      throws TapisException
	{
        // ------------------------- Complete Input ----------------------
        // Fill in Job fields that we assure.
		if (StringUtils.isBlank(job.getLastMessage())) job.setLastMessage(JOB_CREATE_MSG);
		if (job.getCreated() == null) {
	        Instant now = Instant.now();
	        job.setCreated(now);
	        job.setLastUpdated(now);
		}
        
        // ------------------------- Check Input -------------------------
        // Exceptions can be throw from here.
        validateNewJob(job);
	
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Insert into the aloe-jobs table first.
          // Create the command using table definition field order.
          String sql = SqlStatements.CREATE_JOB;
          
          // Prepare the statement and fill in the placeholders.
          // The fields that the DB defaults are not set.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, job.getName());
          pstmt.setString(2, job.getOwner());
          pstmt.setString(3, job.getTenant());
          pstmt.setString(4, job.getDescription());
              
          pstmt.setString(5, job.getStatus().name());
              
          pstmt.setString(6, job.getLastMessage());
          pstmt.setTimestamp(7, Timestamp.from(job.getCreated()));
          pstmt.setTimestamp(8, Timestamp.from(job.getLastUpdated()));
              
          pstmt.setString(9, job.getUuid());
            
          pstmt.setString(10, job.getAppId().trim());
          pstmt.setString(11, job.getAppVersion().trim());
          pstmt.setBoolean(12, job.isArchiveOnAppError());
          pstmt.setBoolean(13, job.isDynamicExecSystem());
              
          pstmt.setString(14, job.getExecSystemId());           
          pstmt.setString(15, job.getExecSystemExecDir());      // could be null
          pstmt.setString(16, job.getExecSystemInputDir());     // could be null
          pstmt.setString(17, job.getExecSystemOutputDir());    // could be null
          pstmt.setString(18, job.getExecSystemLogicalQueue()); // could be null
          
          pstmt.setString(19, job.getArchiveSystemId());        // could be null
          pstmt.setString(20, job.getArchiveSystemDir());       // could be null
              
          pstmt.setString(21, job.getDtnSystemId());            // could be null       
          pstmt.setString(22, job.getDtnMountSourcePath());     // could be null
          pstmt.setString(23, job.getDtnMountPoint());          // could be null
          
          pstmt.setInt(24, job.getNodeCount());
          pstmt.setInt(25, job.getCoresPerNode());
          pstmt.setInt(26, job.getMemoryMB());
          pstmt.setInt(27, job.getMaxMinutes());
              
          pstmt.setString(28, job.getFileInputs());                 
          pstmt.setString(29, job.getParameterSet());             
          pstmt.setString(30, job.getExecSystemConstraints());                 
          pstmt.setString(31, job.getSubscriptions());             

          pstmt.setString(32, job.getTapisQueue());
          pstmt.setString(33, job.getCreatedby());
          pstmt.setString(34, job.getCreatedbyTenant());
          
          var tags = job.getTags();
          Array tagsArray;
          if (tags == null || tags.isEmpty()) 
              tagsArray = conn.createArrayOf("text", new String[0]);
            else {
                String[] sarray = tags.toArray(new String[tags.size()]);
                tagsArray = conn.createArrayOf("text", sarray);
            }
          pstmt.setArray(35, tagsArray);
              
          // Issue the call and clean up statement.
          int rows = pstmt.executeUpdate();
          if (rows != 1) _log.warn(MsgUtils.getMsg("DB_INSERT_UNEXPECTED_ROWS", "jobs", rows, 1));
          pstmt.close();
    
          // Commit the transaction that may include changes to both tables.
          conn.commit();
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_CREATE_ERROR", job.getName(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            _log.error(msg, e);
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
            if (conn != null) 
                try {conn.close();}
                  catch (Exception e) 
                  {
                      // If commit worked, we can swallow the exception.  
                      // If not, the commit exception will be thrown.
                      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                      _log.error(msg, e);
                  }
        }
	}
	
    /* ---------------------------------------------------------------------- */
    /* setStatus:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Set the status of the specified job after checking that the transition
     * from the current status to the new status is legal.  This method commits
     * the update and returns the last update time. 
     * 
     * @param uuid the job whose status is to change    
     * @param newStatus the job's new status
     * @param message the status message to be saved in the job record
     * @return the last update time saved in the job record
     * @throws JobException if the status could not be updated
     */
    public Instant setStatus(String uuid, JobStatusType newStatus, String message)
     throws JobException
    {
        // Check input.
        if (StringUtils.isBlank(uuid)) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "setStatus", "uuid");
            _log.error(msg);
            throw new JobException(msg);
        }

        // Get the job and create its context object for event processing.
        // The new context object is referenced in the job, so it's not garbage.
        Job job = getJobByUUID(uuid);
        Instant ts = setStatus(job, newStatus, message);
        
        return ts;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setStatus:                                                             */
    /* ---------------------------------------------------------------------- */
    /** ALL EXTERNAL STATUS UPDATES AFTER JOB CREATION RUN THROUGH THIS METHOD.
     * 
     * This method sets the status of the specified job after checking that the 
     * transition from the current status to the new status is legal.  This method 
     * commits the update and returns the last update time.
     * 
     * Note that a new job event for this status change is persisted and can
     * trigger notifications to be sent.  Failures in event processing are not
     * exposed as errors to callers, they are performed on a best-effort basis.   
     * 
     * @param uuid the job whose status is to change    
     * @param newStatus the job's new status
     * @param message the status message to be saved in the job record
     * @return the last update time saved in the job record
     * @throws JobException if the status could not be updated
     */
    public Instant setStatus(Job job, JobStatusType newStatus, String message)
     throws JobException
    {
        // ------------------------- Check Input ------------------------
        // We need a job.
        if (job == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "setStatus", "job");
            throw new JobException(msg);
        }
        
        // ------------------------- Change Status ----------------------
        // Call the real method.
        Instant now = Instant.now();
        setStatus(job, newStatus, message, true, now);

        // ------------------------- Send Event -------------------------
        // TODO: SEND EVENTS
        // Create and sent a job event indicating the status change.
//        JobExecutionContext jobCtx = getJobContextSafe(job);
//        try {jobCtx.getJobEventProcessor().processNewStatus(jobCtx, newStatus);}
//            catch (Exception e) {/* already logged */}

        // Return the timestamp that's been saved to the database.
        return now;
    }
    
    /* ---------------------------------------------------------------------- */
    /* failJob:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the specified job to the terminal FAILED state when the caller does
     * not already have a reference to the job object.  If the caller has a job
     * object, use the overloaded version of this method that accepts a job.
     * 
     * @param caller name of calling program or worker
     * @param jobUuid the job to be failed
     * @param tenantId the job's tenant
     * @param failMsg the message to write to the job record
     * @throws JobException 
     */
    public void failJob(String caller, String jobUuid, String tenantId, String failMsg) 
     throws JobException
    {
        // Make sure we write something to the job record.
        if (StringUtils.isBlank(failMsg)) 
            failMsg = MsgUtils.getMsg("JOBS_STATUS_FAILED_UNKNOWN_CAUSE");
        
        // Fail the job.
        try {setStatus(jobUuid, JobStatusType.FAILED, failMsg);}
            catch (Exception e) {
                // The job will be left in a non-terminal state and probably 
                // removed from any queue.  It's likely to become a zombie.
                String msg = MsgUtils.getMsg("JOBS_WORKER_JOB_UPDATE_ERROR", 
                                              caller, jobUuid, tenantId);
                throw new JobException(msg, e);
            }
    }
    
    /* ---------------------------------------------------------------------- */
    /* failJob:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the specified job to the terminal FAILED state when the caller 
     * already has the job object.  This method is more efficient than the 
     * overloaded version that accepts only a uuid.
     * 
     * @param caller name of calling program or worker
     * @param job the job to be failed
     * @param failMsg the message to write to the job record
     * @throws JobException 
     */
    public void failJob(String caller, Job job, String failMsg) 
     throws JobException
    {
        // Make sure we write something to the job record.
        if (StringUtils.isBlank(failMsg)) 
            failMsg = MsgUtils.getMsg("JOBS_STATUS_FAILED_UNKNOWN_CAUSE");
        
        // Fail the job.
        try {setStatus(job, JobStatusType.FAILED, failMsg);}
            catch (Exception e) {
                // The job will be left in a non-terminal state and probably 
                // removed from any queue.  It's likely to become a zombie.
                String msg = MsgUtils.getMsg("JOBS_WORKER_JOB_UPDATE_ERROR", 
                                              caller, job.getUuid(), job.getTenant());
                throw new JobException(msg, e);
            }
    }
    
	/* ---------------------------------------------------------------------- */
	/* queryDB:                                                               */
	/* ---------------------------------------------------------------------- */
	/** Probe connectivity to the specified table.  This method is called during
	 * monitoring so most errors are not logged to avoid filling up our logs.
	 *
	 * @param tableName the tenant id
	 * @return 0 or 1 depending on whether the table is empty or not
	 * @throws TapisException on error
	 */
	public int queryDB(String tableName) 
	  throws TapisException
	{
	    // ------------------------- Check Input -------------------------
		// Exceptions can be throw from here.
		if (StringUtils.isBlank(tableName)) {
		    String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "queryDB", "tableName");
		    throw new TapisException(msg);
		}
		      
		// ------------------------- Call SQL ----------------------------
		int rows = 0;
		Connection conn = null;
		try
		  {
		      // Get a database connection.
		      conn = getConnection();
		          
		      // Get the select command.
		      String sql = SqlStatements.SELECT_1;
		      sql = sql.replace(":table", tableName);
		      
		      // Prepare the statement and fill in the placeholders.
		      PreparedStatement pstmt = conn.prepareStatement(sql);
		                      
		      // Issue the call for the N row result set.
		      ResultSet rs = pstmt.executeQuery();
		      if (rs.next()) rows = rs.getInt(1);
		          
		      // Close the result and statement.
		      rs.close();
		      pstmt.close();
		    
		      // Commit the transaction.
		      conn.commit();
		          
		      // Toggle the last outcome flag if necessary.
		      if (_lastQueryDBSucceeded.toggleOn())
		          _log.info(MsgUtils.getMsg("DB_SELECT_ID_ERROR_CLEARED"));
		  }
		  catch (Exception e)
		  {
		      // Rollback transaction.
		      try {if (conn != null) conn.rollback();}
		          catch (Exception e1){}
		          
		      // Log the first error after a reigning success.
		      String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "tableName", tableName, e.getMessage());
		      if (_lastQueryDBSucceeded.toggleOff()) _log.error(msg, e); 
		      throw new TapisException(msg, e);
		  }
		  finally {
		      // Always return the connection back to the connection pool.
		      try {if (conn != null) conn.close();}
		        catch (Exception e){} 
		  }
		      
		  return rows;
	}

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* setStatus:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Set the status of the specified job after checking that the transition
     * from the current status to the new status is legal.  If the commit flag
     * is true, then the transaction is committed and null is returned.  If the
     * commit flag is false, then the transaction is left uncommitted and the 
     * open connection is returned.  It is the caller's responsibility to commit
     * the transaction and close the connection after issuing any number of 
     * other database calls in the same transaction. 
     * 
     * The in-memory job object is also updated with all changes made to the 
     * database by this method and any method called from this method.
     * 
     * It is the responsibility of the caller or a method earlier in the call 
     * chain to create and process job events.  This method only affects the 
     * jobs table and in-memory job object.
     * 
     * @param uuid the job whose status is to change    
     * @param newStatus the job's new status
     * @param message the status message to be saved in the job record
     * @param commit true to commit the transaction and close the connection;
     *               false to leave the transaction and connection open
     * @param updateTime a specific instant for the last update time or null
     * @return the open connection when the transaction is uncommitted; null otherwise
     * @throws JobException if the status could not be updated
     */
    private Connection setStatus(Job job, JobStatusType newStatus, String message,
                                 boolean commit, Instant updateTime)
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "setStatus", "job");
            throw new JobException(msg);
        }
        
        // Assign the update time if the caller hasn't.
        if (updateTime == null) updateTime = Instant.now();
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
            conn = getConnection();
            
            // --------- Get current status
            // Get the current job status from the database and keep record locked.
            String sql = SqlStatements.SELECT_JOB_STATUS_FOR_UPDATE;
       
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, job.getTenant());
            pstmt.setString(2, job.getUuid());
            
            // Issue the call for the 1 row, 1 field result set.
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) {
                String msg = MsgUtils.getMsg("DB_SELECT_EMPTY_RESULT", sql, 
                                             StringUtils.joinWith(", ", job.getTenant(), job.getUuid()));
                throw new JobException(msg);
            }
                
            // Get current status.
            String curStatusString = rs.getString(1);
            JobStatusType curStatus = JobStatusType.valueOf(curStatusString);
            
            // Debug logging.
            if (_log.isDebugEnabled())
                _log.debug(MsgUtils.getMsg("JOBS_STATUS_UPDATE", job.getUuid(), 
                                           curStatusString, newStatus.name()));

            // --------- Validate requested status transition ---------
            if (!JobFSMUtils.hasTransition(curStatus, newStatus)) {
                String msg = MsgUtils.getMsg("JOBS_STATE_NO_TRANSITION", job.getUuid(), 
                                             curStatusString, newStatus.name());
                throw new JobException(msg);
            }
            // --------------------------------------------------------
            
            // Truncate message if it's longer than the database field length.
            if (message.length() > JOB_LEN_4096) 
               message = message.substring(0, JOB_LEN_4096 - 1);
            
            // Increment the blocked counter if we are transitioning to the blocked state.
            int blockedIncrement = 0;
            if (newStatus == JobStatusType.BLOCKED && curStatus != JobStatusType.BLOCKED)
                blockedIncrement = 1;
            
            // --------- Set new status
            sql = SqlStatements.UPDATE_JOB_STATUS;
            Timestamp ts = Timestamp.from(updateTime);
            
            // Prepare the statement and fill in the placeholders.
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, newStatus.name());
            pstmt.setString(2, message);
            pstmt.setTimestamp(3, ts);
            pstmt.setInt(4, blockedIncrement);
            pstmt.setString(5, job.getTenant());
            pstmt.setString(6, job.getUuid());
            
            // Issue the call.
            int rows = pstmt.executeUpdate();
            if (rows != 1) {
                String parms = StringUtils.joinWith(", ", newStatus.name(), message, ts, 
                                                    blockedIncrement, job.getTenant(), job.getUuid());
                String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
                _log.error(msg);
                throw new JobException(msg);
            }
            
            // Set the remote execution start time when the new status transitions to RUNNING
            // or the job ended time if we have transitioned to a terminal state. The called
            // methods also update the in-memory job object.
            if (newStatus == JobStatusType.RUNNING) updateRemoteStarted(conn, job, ts);
            else if (newStatus.isTerminal()) updateEnded(conn, job, ts);
            
            // Conditionally commit the transaction.
            if (commit) conn.commit();
            
            // Update the in-memory job object.
            job.setStatus(newStatus);
            job.setLastMessage(message);
            job.setLastUpdated(updateTime);
            job.setBlockedCount(job.getBlockedCount() + blockedIncrement);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            // Close and null out the connection here. This overrides the finally block logic and
            // guarantees that we will not interfere with another thread's use of the connection. 
            try {if (conn != null) conn.close(); conn = null;}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", job.getUuid(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Conditionally return the connection back to the connection pool.
            if (commit && (conn != null)) 
                try {conn.close();}
                  catch (Exception e) 
                  {
                      // If commit worked, we can swallow the exception.  
                      // If not, the commit exception will be thrown.
                      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                      _log.error(msg, e);
                  }
        }
        
        // Return the open connection when no commit occurred.
        if (commit) return null;
          else return conn;
    }

    /* ---------------------------------------------------------------------- */
    /* updateRemoteStarted:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Set the remote started timestamp to be equal to the specified timestamp
     * only if the remote started timestamp is null.  This method is really just 
     * an extension of the setStatus() method separated for readability.  
     * 
     * Once set, the remote started timestamp is not updated by this method, so 
     * calling it more than once for a job will not change the job record.
     * 
     * @param conn the connection with the in-progress transaction
     * @param uuid the job uuid
     * @param ts the remote execution start time
     * @throws SQLException
     */
    private void updateRemoteStarted(Connection conn, Job job, Timestamp ts) 
     throws SQLException
    {
        // Set the sql command.
        String sql = SqlStatements.UPDATE_REMOTE_STARTED;
            
        // Prepare the statement and fill in the placeholders.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setTimestamp(1, ts);
        pstmt.setString(2, job.getUuid());
            
        // Issue the call.
        int rows = pstmt.executeUpdate();
        
        // Update the in-memory object.
        job.setRemoteStarted(ts.toInstant());
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateEnded:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Set the ended timestamp to be equal to the specified timestamp
     * only if the ended timestamp is null.  This method is really just 
     * an extension of the setStatus() method separated for readability.  
     * 
     * Once set, the ended timestamp is not updated by this method, so 
     * calling it more than once for a job will not change the job record.
     * 
     * @param conn the connection with the in-progress transaction
     * @param uuid the job uuid
     * @param ts the job termination time
     * @throws SQLException
     */
    private void updateEnded(Connection conn, Job job, Timestamp ts) 
     throws SQLException
    {
        // Set the sql command.
        String sql = SqlStatements.UPDATE_JOB_ENDED;
            
        // Prepare the statement and fill in the placeholders.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setTimestamp(1, ts);
        pstmt.setString(2, job.getUuid());
            
        // Issue the call.
        int rows = pstmt.executeUpdate();
        
        // Update the in-memory object.
        job.setEnded(ts.toInstant());
    }
    
	/* ---------------------------------------------------------------------- */
	/* validateNewJob:                                                        */
	/* ---------------------------------------------------------------------- */
	private void validateNewJob(Job job) throws TapisException
	{
		// Check each field used to create a new job.
		if (StringUtils.isBlank(job.getName())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "name");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getOwner())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "owner");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getTenant())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "tenant");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getDescription())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "description");
	          throw new TapisException(msg);
		}
		
		if (job.getStatus() == null) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "status");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getUuid())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "uuid");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getAppId())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "appId");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getAppVersion())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "appVersion");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getExecSystemId())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemId");
	          throw new TapisException(msg);
		}
		
        if (StringUtils.isBlank(job.getExecSystemExecDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemExecDir");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getExecSystemInputDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemInputDir");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getExecSystemOutputDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemOutputDir");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getArchiveSystemId())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "archiveSystemId");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getArchiveSystemDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "archiveSystemDir");
            throw new TapisException(msg);
        }
      
		// For flexibility, we allow the hpc scheduler to deal with validating resource reservation values.
		if (job.getMaxMinutes() < 1) {
	          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateNewJob", "maxMinutes", job.getMaxMinutes());
	          throw new TapisException(msg);
		}

		if (StringUtils.isBlank(job.getFileInputs())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "fileInputs");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getParameterSet())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "parameterSet");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getExecSystemConstraints())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemConstraints");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getSubscriptions())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "notifications");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getTapisQueue())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "tapisQueue");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getCreatedby())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "createdBy");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getCreatedbyTenant())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "createdByTenant");
	          throw new TapisException(msg);
		}
	}
	
	/* ---------------------------------------------------------------------- */
	/* populateJobs:                                                          */
	/* ---------------------------------------------------------------------- */
	/** Populate a new Jobs object with a record retrieved from the 
	 * database.  The result set's cursor will be advanced to the next
	 * position and, if a row exists, its data will be marshalled into a 
	 * Jobs object.  The result set is not closed by this method.
	 * 
	 * NOTE: This method assumes all fields are returned table definition order.
	 * 
	 * NOTE: This method must be manually maintained whenever the table schema changes.  
	 * 
	 * @param rs the unprocessed result set from a query.
	 * @return a new model object or null if the result set is null or empty
	 * @throws AloeJDBCException on SQL access or conversion errors
	 */
	private Job populateJobs(ResultSet rs)
	 throws TapisJDBCException
	{
	    // Quick check.
	    if (rs == null) return null;
	    
	    try {
	      // Return null if the results are empty or exhausted.
	      // This call advances the cursor.
	      if (!rs.next()) return null;
	    }
	    catch (Exception e) {
	      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
	      throw new TapisJDBCException(msg, e);
	    }
	    
	    // Populate the Jobs object using table definition field order,
	    // which is the order specified in all calling methods.
	    Job obj = new Job();
	    try {
	        obj.setId(rs.getInt(1));
	        obj.setName(rs.getString(2));
	        obj.setOwner(rs.getString(3));
	        obj.setTenant(rs.getString(4));
	        obj.setDescription(rs.getString(5));
	        obj.setStatus(JobStatusType.valueOf(rs.getString(6)));
	        obj.setLastMessage(rs.getString(7));
	        obj.setCreated(rs.getTimestamp(8).toInstant());

	        Timestamp ts = rs.getTimestamp(9);
	        if (ts != null) obj.setEnded(ts.toInstant());

	        obj.setLastUpdated(rs.getTimestamp(10).toInstant());
	        obj.setUuid(rs.getString(11));
	        obj.setAppId(rs.getString(12));
	        obj.setAppVersion(rs.getString(13));
	        obj.setArchiveOnAppError(rs.getBoolean(14));
	        obj.setDynamicExecSystem(rs.getBoolean(15));
	        
	        obj.setExecSystemId(rs.getString(16));
	        obj.setExecSystemExecDir(rs.getString(17));
	        obj.setExecSystemInputDir(rs.getString(18));
	        obj.setExecSystemOutputDir(rs.getString(19));
	        obj.setExecSystemLogicalQueue(rs.getString(20));
	        
	        obj.setArchiveSystemId(rs.getString(21));
	        obj.setArchiveSystemDir(rs.getString(22));
	        
	        obj.setDtnSystemId(rs.getString(23));
	        obj.setDtnMountSourcePath(rs.getString(24));
	        obj.setDtnMountPoint(rs.getString(25));
	        
	        obj.setNodeCount(rs.getInt(26));
	        obj.setCoresPerNode(rs.getInt(27));
	        obj.setMemoryMB(rs.getInt(28));
	        obj.setMaxMinutes(rs.getInt(29));
	        
	        obj.setFileInputs(rs.getString(30));
	        obj.setParameterSet(rs.getString(31));
	        obj.setExecSystemConstraints(rs.getString(32));
	        obj.setSubscriptions(rs.getString(33));	        
	        
	        obj.setBlockedCount(rs.getInt(34));
	        obj.setRemoteJobId(rs.getString(35));
	        obj.setRemoteJobId2(rs.getString(36));
	        
	        String s = rs.getString(37);
	        if (s != null) obj.setRemoteOutcome(JobRemoteOutcome.valueOf(s));
	        obj.setRemoteResultInfo(rs.getString(38));
	        obj.setRemoteQueue(rs.getString(39));

	        ts = rs.getTimestamp(40);
	        if (ts != null) obj.setRemoteSubmitted(ts.toInstant());

	        ts = rs.getTimestamp(41);
	        if (ts != null) obj.setRemoteStarted(ts.toInstant());

	        ts = rs.getTimestamp(42);
	        if (ts != null) obj.setRemoteEnded(ts.toInstant());

	        obj.setRemoteSubmitRetries(rs.getInt(43));
	        obj.setRemoteChecksSuccess(rs.getInt(44));
	        obj.setRemoteChecksFailed(rs.getInt(45));

	        ts = rs.getTimestamp(46);
	        if (ts != null) obj.setRemoteLastStatusCheck(ts.toInstant());

	        obj.setTapisQueue(rs.getString(47));
	        obj.setVisible(rs.getBoolean(48));
	        obj.setCreatedby(rs.getString(49));
	        obj.setCreatedbyTenant(rs.getString(50));
	        
	        Array tagsArray = rs.getArray(51);
	        if (tagsArray != null) {
	            var stringArray = (String[])tagsArray.getArray();
	            if (stringArray != null && stringArray.length > 0) { 
	                var tagsSet = new TreeSet<String>();
	                for (String s1 : stringArray) tagsSet.add(s1);
	                obj.setTags(tagsSet);
	            }
	        } 
	    } 
	    catch (Exception e) {
	      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
	      throw new TapisJDBCException(msg, e);
	    }
	      
	    return obj;
	}
}

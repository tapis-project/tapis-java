package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
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
	  throws TapisException
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
	          _log.error(msg, e);
	          throw new TapisException(msg, e);
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
	/* getJobsByUUID:                                                         */
	/* ---------------------------------------------------------------------- */
	public Job getJobsByUUID(String uuid) 
	  throws TapisException
	{
	    // ------------------------- Check Input -------------------------
	    if (StringUtils.isBlank(uuid)) {
	        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobsByUUID", "uuid");
	        _log.error(msg);
	        throw new TapisException(msg);
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
	          _log.error(msg, e);
	          throw new TapisException(msg, e);
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
        // Fill in Job fields that we control.
		if (StringUtils.isBlank(job.getLastMessage())) job.setLastMessage(JOB_CREATE_MSG);
        Instant now = Instant.now();
        job.setCreated(now);
        job.setLastUpdated(now);
        
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
          pstmt.setString(1, job.getName().trim());
          pstmt.setString(2, job.getOwner().trim());
          pstmt.setString(3, job.getTenant().trim());
          pstmt.setString(4, job.getDescription().trim());
              
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
            throw new JobQueueException(msg, e);
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
		    _log.error(msg);
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
	      _log.error(msg, e);
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
	      _log.error(msg, e);
	      throw new TapisJDBCException(msg, e);
	    }
	      
	    return obj;
	}
}

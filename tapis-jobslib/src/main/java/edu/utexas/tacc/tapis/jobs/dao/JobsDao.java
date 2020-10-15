package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobExecClass;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;

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
        // ------------------------- Check Input -------------------------
        // Exceptions can be throw from here.
        validateJob(job);
	
        // ------------------------- Complete Input ----------------------
        // Fill in Job fields that we control.
        Instant now = Instant.now();
        job.setCreated(now);
        job.setLastUpdated(now);
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Insert into the aloe-jobs table first.
          // Create the command using table definition field order.
          String sql = SqlStatements.CREATE_JOB;
          

//              name, owner, tenant, description, status, type, exec_class, 
//          	  last_message, created, last_updated, uuid, app_id, app_version, 
//          	  archive_on_app_error, input_system_id, exec_system_id, exec_system_exec_path, 
//              exec_system_input_path, archive_system_id, archive_system_path, nodes, 
//              processors_per_node, memory_mb, max_minutes, inputs, parameters, events, 
//              exec_system_constraints, tapis_queue, createdby, createdby_tenant 

              
          // Prepare the statement and fill in the placeholders.
          // The fields that the DB defaults are not set.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, job.getName().trim());
          pstmt.setString(2, job.getOwner().trim());
          pstmt.setString(3, job.getTenant().trim());
          pstmt.setString(4, job.getDescription().trim());
              
          pstmt.setString(5, job.getStatus().name());
          pstmt.setString(6, job.getType().name());
          pstmt.setString(7, job.getExecClass().name());
              
          pstmt.setString(8, job.getLastMessage());
          pstmt.setTimestamp(9, Timestamp.from(job.getCreated()));
          pstmt.setTimestamp(10, Timestamp.from(job.getLastUpdated()));
              
          pstmt.setString(11, job.getUuid());
            
          pstmt.setString(12, job.getAppId().trim());
          pstmt.setString(13, job.getAppVersion().trim());
          pstmt.setBoolean(14, job.isArchiveOnAppError());
              
          pstmt.setString(15, job.getInputSystemId());           // could be null
          pstmt.setString(16, job.getExecSystemId());           // could be null
          pstmt.setString(17, job.getExecSystemExecPath());           // could be null
          pstmt.setString(18, job.getExecSystemInputPath());           // could be null
          pstmt.setString(19, job.getArchiveSystemId());           // could be null
          pstmt.setString(20, job.getArchiveSystemPath());           // could be null
              
          pstmt.setInt(21, job.getNodes());
          pstmt.setInt(22, job.getProcessorsPerNode());
          pstmt.setInt(23, job.getMemoryMb());
          pstmt.setInt(24, job.getMaxMinutes());
              
          pstmt.setString(25, job.getInputs());
          pstmt.setString(26, job.getParameters());
          pstmt.setString(27, job.getEvents());
          pstmt.setString(28, job.getExecSystemConstraints());

          pstmt.setString(29, job.getTapisQueue());
          pstmt.setString(30, job.getCreatedby());
          pstmt.setString(31, job.getCreatedbyTenant());
              
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
	/* validateJob:                                                           */
	/* ---------------------------------------------------------------------- */
	private void validateJob(Job job)
	{
		
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
	        obj.setStatus(JobStatusType.valueOf(rs.getString(10)));
	        obj.setType(JobType.valueOf(rs.getString(7)));
	        obj.setExecClass(JobExecClass.valueOf(rs.getString(8)));
	        obj.setLastMessage(rs.getString(9));
	        obj.setCreated(rs.getTimestamp(10).toInstant());

	        Timestamp ts = rs.getTimestamp(11);
	        if (ts != null) obj.setEnded(ts.toInstant());

	        obj.setLastUpdated(rs.getTimestamp(12).toInstant());
	        obj.setUuid(rs.getString(13));
	        obj.setAppId(rs.getString(14));
	        obj.setAppVersion(rs.getString(15));
	        obj.setArchiveOnAppError(rs.getBoolean(16));
	        obj.setInputSystemId(rs.getString(17));
	        obj.setExecSystemId(rs.getString(18));
	        obj.setExecSystemExecPath(rs.getString(19));
	        obj.setExecSystemInputPath(rs.getString(20));
	        obj.setArchiveSystemId(rs.getString(21));
	        obj.setArchiveSystemPath(rs.getString(22));
	        obj.setNodes(rs.getInt(23));
	        obj.setProcessorsPerNode(rs.getInt(24));
	        obj.setMemoryMb(rs.getInt(25));
	        obj.setMaxMinutes(rs.getInt(26));
	        obj.setInputs(rs.getString(27));
	        obj.setParameters(rs.getString(28));
	        obj.setEvents(rs.getString(29));
	        obj.setExecSystemConstraints(rs.getString(30));
	        obj.setBlockedCount(rs.getInt(31));
	        obj.setRemoteJobId(rs.getString(32));
	        obj.setRemoteJobId2(rs.getString(33));
	        obj.setRemoteOutcome(JobRemoteOutcome.valueOf(rs.getString(34)));
	        obj.setRemoteResultInfo(rs.getString(35));
	        obj.setRemoteQueue(rs.getString(36));

	        ts = rs.getTimestamp(37);
	        if (ts != null) obj.setRemoteSubmitted(ts.toInstant());

	        ts = rs.getTimestamp(38);
	        if (ts != null) obj.setRemoteStarted(ts.toInstant());

	        ts = rs.getTimestamp(39);
	        if (ts != null) obj.setRemoteEnded(ts.toInstant());

	        obj.setRemoteSubmitRetries(rs.getInt(40));
	        obj.setRemoteChecksSuccess(rs.getInt(41));
	        obj.setRemoteChecksFailed(rs.getInt(42));

	        ts = rs.getTimestamp(43);
	        if (ts != null) obj.setRemoteLastStatusCheck(ts.toInstant());

	        obj.setTapisQueue(rs.getString(44));
	        obj.setVisible(rs.getBoolean(45));
	        obj.setCreatedby(rs.getString(46));
	        obj.setCreatedbyTenant(rs.getString(47));
	    } 
	    catch (Exception e) {
	      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
	      _log.error(msg, e);
	      throw new TapisJDBCException(msg, e);
	    }
	      
	    return obj;
	}
}

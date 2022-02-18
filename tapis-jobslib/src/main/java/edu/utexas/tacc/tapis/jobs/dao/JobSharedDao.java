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
import edu.utexas.tacc.tapis.jobs.events.JobEventManager;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobSharedDao extends AbstractDao{
	
	  /* ********************************************************************** */
	  /*                               Constants                                */
	  /* ********************************************************************** */
	  // Tracing.
	  private static final Logger _log = LoggerFactory.getLogger(JobSharedDao.class);
	  
	  /* ********************************************************************** */
	  /*                                 Fields                                 */
	  /* ********************************************************************** */
	  
	  /* ********************************************************************** */
	  /*                              Constructors                              */
	  /* ********************************************************************** */
	  /* ---------------------------------------------------------------------- */
	  /* constructor:                                                           */
	  /* ---------------------------------------------------------------------- */
	  /** This class depends on the calling code to provide a datasource for
	   * db connections since this code in not part of a free-standing service.
	   * 
	   * @param dataSource the non-null datasource 
	   */
	  public JobSharedDao() throws TapisException {}
	  
	  /* ********************************************************************** */
	  /*                             Public Methods                             */
	  /* ********************************************************************** */
	  /* ---------------------------------------------------------------------- */
	  /* getJobEvents:                                                       */
	  /* ---------------------------------------------------------------------- */
	  public List<JobShared> getJobsSharedWith(String tenant, String user_shared_with) 
	    throws TapisException
	  {
	      // Initialize result.
	      ArrayList<JobShared> list = new ArrayList<>();

	      // ------------------------- Call SQL ----------------------------
	      Connection conn = null;
	      try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_SHARED_WITH_USER;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, tenant);
	          pstmt.setString(2, user_shared_with);
	         
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          JobShared obj = populateJobShared(rs);
	          while (obj != null) {
	            list.add(obj);
	            obj = populateJobShared(rs);
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
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobShared", "allUUIDs", e.getMessage());
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
	  /* getJobEventsByJobUuid:                                                 */
	  /* ---------------------------------------------------------------------- */
	  public List<JobShared> getJobSharedByJobUUID(String jobUuid, int limit, int skip) 
	    throws TapisException
	  {
	      // Initialize result.
	      ArrayList<JobShared> list = new ArrayList<>();

	      // ------------------------- Call SQL ----------------------------
	      Connection conn = null;
	      try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_SHARED_BY_JOB_UUID;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, jobUuid);
	          //pstmt.setInt(2, limit);
	          //pstmt.setInt(3, skip);
	         
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          JobShared obj = populateJobShared(rs);
	          while (obj != null) {
	            list.add(obj);
	            obj = populateJobShared(rs);
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
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobShared", jobUuid, e.getMessage());
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
	  /* createJobShared:                                                       */
	  /* ---------------------------------------------------------------------- */
	  public void createSharedJob(JobShared jobShared)
	    throws TapisException
	  {
	      // ------------------------- Complete Input ----------------------
	      // Fill in Job fields that we assure.
		  jobShared.setCreated(Instant.now());
	      jobShared.setLastUpdated(Instant.now());
	      // ------------------------- Check Input -------------------------
		  validateNewSharedJob(jobShared);
	     
	      
	      // ------------------------- Call SQL ----------------------------
		  //JobShared jobShare = null;
	      Connection conn = null;
	      try
	      {
	         // Get a database connection.
	         conn = getConnection();


	        // Insert into the jobs table first.
	        // Create the command using table definition field order.
	        String sql = SqlStatements.CREATE_JOB_SHARED;
	 	        
	        // Prepare the statement and fill in the placeholders.
	        // The fields that the DB defaults are not set.
	        PreparedStatement pstmt = conn.prepareStatement(sql);
	        pstmt.setString(1, jobShared.getTenant());
	        pstmt.setString(2, jobShared.getCreatedby());
	        pstmt.setString(3, jobShared.getJobUuid());
	        pstmt.setString(4, jobShared.getUserSharedWith());
	        pstmt.setString(5, jobShared.getJobResource().name());
	        pstmt.setString(6, jobShared.getJobPermission().name());  
	        pstmt.setTimestamp(7, Timestamp.from(jobShared.getCreated()));
	        pstmt.setTimestamp(8, Timestamp.from(jobShared.getLastUpdated()));
	       
           // Issue the call and clean up statement.
           int rows = pstmt.executeUpdate();
           if (rows != 1) _log.warn(MsgUtils.getMsg("DB_INSERT_UNEXPECTED_ROWS", "jobs_shared", rows, 1));
          pstmt.close();
          
          // Write the event table and issue the notification.
          //TODO Add JobEvent for Sharing
          var eventMgr = JobEventManager.getInstance();
        
          //eventMgr.recordStatusEvent(jobShared.getJobUuid(), job.getStatus(), null, conn);
    
          // Commit the transaction that may include changes to both tables.
          conn.commit();
	        }
	        catch (Exception e)
	        {
	            // Rollback transaction.
	            try {if (conn != null) conn.rollback();}
	                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	            
	            String msg = MsgUtils.getMsg("JOBS_JOB_SHARED_CREATE_ERROR", jobShared.getJobUuid(), 
	            		jobShared.getTenant(), jobShared.getCreatedby(), jobShared.getUserSharedWith(),e.getMessage());
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

	  /* ********************************************************************** */
	  /*                             Private Methods                            */
	  /* ********************************************************************** */

       private  void validateNewSharedJob(JobShared jobShared)
    		   throws TapisException 
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
 	      if (StringUtils.isBlank(jobShared.getUserSharedWith())) {
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
	  /* ---------------------------------------------------------------------- */
	  /* populateJobShared:                                                     */
	  /* ---------------------------------------------------------------------- */
	  /** Populate a new JobEvents object with a record retrieved from the 
	   * database.  The result set's cursor will be advanced to the next
	   * position and, if a row exists, its data will be marshalled into a 
	   * JobEvents object.  The result set is not closed by this method.
	   * 
	   * NOTE: This method assumes all fields are returned table definition order.
	   * 
	   * NOTE: This method must be manually maintained whenever the table schema changes.  
	   * 
	   * @param rs the unprocessed result set from a query.
	   * @return a new model object or null if the result set is null or empty
	   * @throws TapisJDBCException on SQL access or conversion errors
	   */
	  private JobShared populateJobShared(ResultSet rs)
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
	    
	    // Populate the JobEvents object using table definition field order,
	    // which is the order specified in all calling methods.
	    JobShared obj = new JobShared();
	    try {
	    	
	    	obj.setId(rs.getInt(1));
	    	obj.setTenant(rs.getString(2));
	    	obj.setCreatedby(rs.getString(3));
	    	obj.setJobUuid(rs.getString(4));
	    	obj.setUserSharedWith(rs.getString(5));
	    	obj.setJobResource(JobResourceShare.valueOf(rs.getString(6)));
	    	obj.setJobPermission(JobTapisPermission.valueOf(rs.getString(7)));
	    	obj.setCreated(rs.getTimestamp(8).toInstant());
	    	obj.setLastUpdated(rs.getTimestamp(9).toInstant());
	       
	    } 
	    catch (Exception e) {
	      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
	      _log.error(msg, e);
	      throw new TapisJDBCException(msg, e);
	    }
	      
	    return obj;
	  }

}

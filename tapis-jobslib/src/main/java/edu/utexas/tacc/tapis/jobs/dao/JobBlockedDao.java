package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobBlocked;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class JobBlockedDao
 extends AbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobBlockedDao.class);
  
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
  public JobBlockedDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getJobBlocked:                                                       */
  /* ---------------------------------------------------------------------- */
  public List<JobBlocked> getJobBlocked() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<JobBlocked> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_JOBBLOCKED;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          JobBlocked obj = populateJobBlocked(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateJobBlocked(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobBlocked", "allUUIDs", e.getMessage());
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
  /* getBlockedJobs:                                                        */
  /* ---------------------------------------------------------------------- */
  public List<JobBlocked> getBlockedJobs(long recoveryId) 
   throws JobException
  {
      // ------------------------- Call SQL ----------------------------
      ArrayList<JobBlocked> list = new ArrayList<>();
      Connection conn = null;
      PreparedStatement pstmt = null;
      ResultSet rs = null;
      try
      {
        // Get a database connection.
        conn = getConnection();
        
        // Create the command using table definition field order
        // in each row and descending next attempt row order.
        String sql = SqlStatements.SELECT_BLOCKED_JOBS_BY_RECOVERY_ID;

        // Prepare the statement and fill in the placeholders.
        pstmt = conn.prepareStatement(sql);
        pstmt.setLong(1, recoveryId);
        
        // Execute the query.
        rs = pstmt.executeQuery();
        JobBlocked obj = populateJobBlocked(rs);
        while (obj != null) {
          list.add(obj);
          obj = populateJobBlocked(rs);
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
          
          String msg = MsgUtils.getMsg("JOBS_RECOVERY_SELECT_ALL_ERROR", e.getMessage());
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
      
      // Non-null result.
      return list;
  }
  
  /* ---------------------------------------------------------------------- */
  /* addBlockedJob:                                                         */
  /* ---------------------------------------------------------------------- */
  void addBlockedJob(Connection conn, JobBlocked blocked) 
   throws SQLException
  {
      PreparedStatement pstmt = null;
      try {
          // Create the command using table definition field order.
          String sql = SqlStatements.CREATE_BLOCKED_JOB;
  
          // Prepare the statement and fill in the placeholders.
          pstmt = conn.prepareStatement(sql);
          pstmt.setLong(1, blocked.getRecoveryId());
          pstmt.setTimestamp(2, Timestamp.from(blocked.getCreated()));
          pstmt.setString(3, blocked.getSuccessStatus().name());
          pstmt.setString(4, blocked.getJobUuid());
          pstmt.setString(5, blocked.getStatusMessage());
      
          // Issue the call.
          int rows = pstmt.executeUpdate();
      }
      catch (SQLIntegrityConstraintViolationException e) {
          // Duplicate records are tolerated. The existing blocked record is preserved.
          // The calling method treats this exception as a special case, not a failure.
          // We swallow the exception to make it seem to the caller like the insertion
          // succeeded.  
          //
          // See last commit before 12/6/2018 for debug code that queries existing record.
          String msg = MsgUtils.getMsg("JOBS_DUPLICATE_BLOCKED_WARN", blocked.getJobUuid(),
                       blocked.getRecoveryId(), blocked.getCreated().toString(), blocked.getSuccessStatus(), 
                       blocked.getStatusMessage().substring(0, Math.min(35, blocked.getStatusMessage().length())),
                       e.getMessage());
          _log.warn(msg, e);
      }
      catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_CREATE_BLOCKED_ERROR", blocked.getJobUuid(), 
                                       blocked.getRecoveryId(), e.getMessage());
          _log.error(msg, e);
          throw e;
      }
      finally {
          // Clean up db resources.
          if (pstmt != null) try {pstmt.close();} catch (Exception e) {}
      }
  }
  
  /* ---------------------------------------------------------------------- */
  /* deleteBlockedJob:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Delete a blocked job from the blocked job table.
   * 
   * @param jobUuid the user's job uuid
   * @throws JobException 
   */
  public void deleteBlockedJob(String jobUuid) 
   throws JobException
  {
      // ------------------------- Tracing -----------------------------
      if (_log.isDebugEnabled()) {
          String msg = MsgUtils.getMsg("JOBS_RECOVERY_DELETING_BLOCKED_JOB", jobUuid);
          _log.debug(msg);
      }

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
        // Get a database connection.
        conn = getConnection();

        // Create the command using table definition field order.
        String sql = SqlStatements.DELETE_BLOCKED_JOB;

        // Prepare the statement and fill in the placeholders.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, jobUuid);
    
        // Issue the call.
        int rows = pstmt.executeUpdate();
        
        // Release resources.
        pstmt.close();
        
        // Commit everything.
        conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("JOBS_RECOVERY_DELETE_BLOCKED_JOB", jobUuid, e.getMessage());
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
  /* ---------------------------------------------------------------------- */
  /* populateJobBlocked:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Populate a new JobBlocked object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * JobBlocked object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws TapisJDBCException on SQL access or conversion errors
   */
  private JobBlocked populateJobBlocked(ResultSet rs)
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
    
    // Populate the JobBlocked object using table definition field order,
    // which is the order specified in all calling methods.
    JobBlocked obj = new JobBlocked();
    try {
        obj.setId(rs.getInt(1));
        obj.setRecoveryId(rs.getInt(2));
        obj.setCreated(rs.getTimestamp(3).toInstant());
        obj.setSuccessStatus(JobStatusType.valueOf(rs.getString(4)));
        obj.setJobUuid(rs.getString(5));
        obj.setStatusMessage(rs.getString(6));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
}

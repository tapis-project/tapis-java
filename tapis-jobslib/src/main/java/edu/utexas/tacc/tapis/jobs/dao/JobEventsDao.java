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
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class JobEventsDao
 extends AbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobEventsDao.class);
  
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
  public JobEventsDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getJobEvents:                                                       */
  /* ---------------------------------------------------------------------- */
  public List<JobEvent> getJobEvents() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<JobEvent> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_JOBEVENTS;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          JobEvent obj = populateJobEvents(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateJobEvents(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobEvents", "allUUIDs", e.getMessage());
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
  public List<JobEvent> getJobEventsByJobUUID(String jobUuid, int limit, int skip) 
    throws TapisException
  {
      // Initialize result.
      ArrayList<JobEvent> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_JOBEVENTS_BY_JOB_UUID;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, jobUuid);
          pstmt.setInt(2, limit);
          pstmt.setInt(3, skip);
         
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          JobEvent obj = populateJobEvents(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateJobEvents(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobEvents", jobUuid, e.getMessage());
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
  /* createEvent:                                                           */
  /* ---------------------------------------------------------------------- */
  public void createEvent(JobEvent jobEvent, Connection callerConn)
    throws TapisException
  {
      // ------------------------- Complete Input ----------------------
      // Fill in Job fields that we assure.
      jobEvent.setCreated(Instant.now());
      
      // ------------------------- Check Input -------------------------
      if (StringUtils.isBlank(jobEvent.getJobUuid())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createEvent", "jobUuid");
          throw new JobException(msg);
      }
      if (StringUtils.isBlank(jobEvent.getDescription())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createEvent", "description");
          throw new JobException(msg);
      }
      if (jobEvent.getEvent() == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createEvent", "event");
          throw new JobException(msg);
      }
      if (jobEvent.getEventDetail() == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createEvent", "eventDetail");
          throw new JobException(msg);
      }
      if (jobEvent.getEvent() == JobEventType.JOB_INPUT_TRANSACTION_ID ||
          jobEvent.getEvent() == JobEventType.JOB_ARCHIVE_TRANSACTION_ID) {
          if (StringUtils.isBlank(jobEvent.getOthUuid())) {
              String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createEvent", "othUuid");
              throw new JobException(msg);
          }
      }
      
      // ------------------------- Call SQL ----------------------------
      boolean usingCallerConn = callerConn != null;
      Connection conn = callerConn;
      try
      {
        // Get a database connection.
        if (!usingCallerConn) conn = getConnection();

        // Insert into the jobs table first.
        // Create the command using table definition field order.
        String sql = SqlStatements.CREATE_JOB_EVENT;
        
        // Prepare the statement and fill in the placeholders.
        // The fields that the DB defaults are not set.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, jobEvent.getEvent().name());
        pstmt.setTimestamp(2, Timestamp.from(jobEvent.getCreated()));
        pstmt.setString(3, jobEvent.getJobUuid());
        pstmt.setString(4, jobEvent.getEventDetail());
        pstmt.setString(5, jobEvent.getOthUuid());  // can be null
        pstmt.setString(6, jobEvent.getDescription());
        
        // Issue the call and clean up statement.
        int rows = pstmt.executeUpdate();
        if (rows != 1) _log.warn(MsgUtils.getMsg("DB_INSERT_UNEXPECTED_ROWS", "jobEvents", rows, 1));
        pstmt.close();
  
        // Commit the transaction that may include changes to both tables.
        if (!usingCallerConn) conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (!usingCallerConn && conn != null) conn.rollback();}
              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("JOBS_CREATE_JOB_EVENT", jobEvent.getEvent().name(), 
                                       jobEvent.getJobUuid(), e.getMessage());
          throw new JobException(msg, e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
          if (!usingCallerConn && conn != null) 
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
  /* populateJobEvents:                                                  */
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
  private JobEvent populateJobEvents(ResultSet rs)
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
    JobEvent obj = new JobEvent();
    try {
        obj.setId(rs.getLong(1));
        obj.setEvent(JobEventType.valueOf(rs.getString(2)));
        obj.setCreated(rs.getTimestamp(3).toInstant());
        obj.setJobUuid(rs.getString(4));
        obj.setEventDetail(rs.getString(5));
        obj.setOthUuid(rs.getString(6)); // can be null
        obj.setDescription(rs.getString(7));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
}

package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.model.JobEvents;
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
  public List<JobEvents> getJobEvents() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<JobEvents> list = new ArrayList<>();

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
          JobEvents obj = populateJobEvents(rs);
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
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private JobEvents populateJobEvents(ResultSet rs)
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
    JobEvents obj = new JobEvents();
    try {
        obj.setId(rs.getInt(1));
        obj.setCreated(rs.getTimestamp(2).toInstant());
        obj.setJobUuid(rs.getString(3));
        obj.setOthUuid(rs.getString(4));
        obj.setEvent(rs.getString(5));
        obj.setDescription(rs.getString(6));

    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
}

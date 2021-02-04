package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueFilterException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueuePriorityException;
import edu.utexas.tacc.tapis.jobs.model.JobQueue;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.SelectorFilter;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class JobQueuesDao
  extends AbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobQueuesDao.class);
  
  // The default queue name and priority for all tenants.
  public static final int DEFAULT_TENANT_QUEUE_PRIORITY = 1;
  
  // Rabbitmq limit.
  public static final int MAX_QUEUE_NAME_LEN = 255;
  
  // From the amqp-0-9-1 reference:  The queue name can be empty, or a sequence of 
  // these characters: letters, digits, hyphen, underscore, period, or colon.
  private static Pattern _queueNamePattern = Pattern.compile("(\\w|\\.|-|_|:)+");
  
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
  public JobQueuesDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getJobQueueByName:                                                     */
  /* ---------------------------------------------------------------------- */
  /** Return the definition of the named queue or null if not found.
   * 
   * @param queueName the name of the queue to search
   * @return the queue definition or null if not found
   * @throws TapisException
   */
  public JobQueue getJobQueueByName(String queueName) 
    throws TapisException
  {
      // Initialize result.
      JobQueue queue = null;

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_JOBQUEUE_BY_NAME;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, queueName);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          queue = populateJobQueues(rs);
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobQueues", "allUUIDs", e.getMessage());
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
      
      return queue;
  }

  /* ---------------------------------------------------------------------- */
  /* getJobQueuesByPriorityDesc:                                            */
  /* ---------------------------------------------------------------------- */
  /** Return a non-empty list of queues.  
   * 
   * @return the non-empty list of defined queues
   * @throws TapisException on error or if the result list is empty
   */
  public List<JobQueue> getJobQueuesByPriorityDesc() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<JobQueue> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_JOBQUEUES_BY_PRIORITY_DESC;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          JobQueue obj = populateJobQueues(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateJobQueues(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "JobQueues", "allUUIDs", e.getMessage());
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
      
      // Throw an exception is the list is empty.
      if (list.isEmpty()) {
          String msg = MsgUtils.getMsg("JOBS_QUEUE_NO_QUEUES");
          throw new TapisException(msg);
      }
      
      return list;
  }

  /* ---------------------------------------------------------------------- */
  /* createQueue:                                                           */
  /* ---------------------------------------------------------------------- */
  public void createQueue(JobQueue queue) throws TapisException
  {
      // ------------------------- Complete Input ----------------------
      // Fill in any missing queue fields.
      if (queue.getCreated() == null) {
          Instant now = Instant.now();
          queue.setCreated(now);
          queue.setLastUpdated(now);
      }
      if (StringUtils.isBlank(queue.getUuid()))
          queue.setUuid(new TapisUUID(UUIDType.JOB_QUEUE).toString());

      // ------------------------- Check Input -------------------------
      // Exceptions can be thrown from here.
      validateJobQueue(queue);
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
        // Get a database connection.
        conn = getConnection();

        // Insert into the job_queue table.
        // Create the command using table definition field order.
        String sql = SqlStatements.CREATE_JOBQUEUE;
        
        // Prepare the statement and fill in the placeholders.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, queue.getName());
        pstmt.setInt(2, queue.getPriority());
        pstmt.setString(3, queue.getFilter());
        pstmt.setString(4, queue.getUuid());
        pstmt.setTimestamp(5, Timestamp.from(queue.getCreated()));
        pstmt.setTimestamp(6, Timestamp.from(queue.getLastUpdated()));
        
        // Issue the call and clean up statement.
        int rows = pstmt.executeUpdate();
        if (rows != 1) _log.warn(MsgUtils.getMsg("DB_INSERT_UNEXPECTED_ROWS", "job_queues", rows, 1));
        pstmt.close();

        // Commit the transaction that may include changes to both tables.
        conn.commit();
        
        // Note that the queue was created.
        _log.info(MsgUtils.getMsg("JOBS_JOB_QUEUE_CREATED", queue.getName()));
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
            catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
        
          String msg = MsgUtils.getMsg("JOBS_JOB_QUEUE_CREATE_ERROR", queue.getName(), e.getMessage());
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

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* populateJobQueues:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Populate a new JobQueues object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * JobQueues object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws TapisJDBCException on SQL access or conversion errors
   */
  private JobQueue populateJobQueues(ResultSet rs)
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
    
    // Populate the JobQueues object using table definition field order,
    // which is the order specified in all calling methods.
    JobQueue obj = new JobQueue();
    try {
        obj.setId(rs.getInt(1));
        obj.setName(rs.getString(2));
        obj.setPriority(rs.getInt(3));
        obj.setFilter(rs.getString(4));
        obj.setUuid(rs.getString(5));
        obj.setCreated(rs.getTimestamp(6).toInstant());
        obj.setLastUpdated(rs.getTimestamp(7).toInstant());
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
  /* ---------------------------------------------------------------------- */
  /* validateJobQueue:                                                      */
  /* ---------------------------------------------------------------------- */
  private void validateJobQueue(JobQueue queue) throws TapisException
  {
      // The uuid is always set by caller, so no need to check here.
      if (StringUtils.isBlank(queue.getName())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createQueue", "name");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(queue.getFilter())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createQueue", "filter");
          throw new TapisException(msg);
      }
      if (queue.getPriority() < 1) {
          String msg =  MsgUtils.getMsg("JOBS_QUEUE_INVALID_PRIORITY");
          throw new JobQueuePriorityException(msg);
      }
      
      // Check queue name for invalid length.
      if (queue.getName().length() > MAX_QUEUE_NAME_LEN) {
          String msg = MsgUtils.getMsg("JOBS_QUEUE_LONG_NAME", queue.getName().substring(0, 64));
          throw new JobQueueException(msg);
      }
          
      // Check queue name for invalid characters.
      Matcher m = _queueNamePattern.matcher(queue.getName());
      if (!m.matches()) {
          String msg = MsgUtils.getMsg("JOBS_QUEUE_INVALID_NAME", queue.getName());
          throw new JobQueueException(msg);
      }
      
      // The queue name must begin with "tapis.jobq.submit."
      String prefix = JobQueueManagerNames.TAPIS_JOBQ_PREFIX + JobQueueManagerNames.SUBMIT_PART;
      if (!queue.getName().startsWith(prefix)) {
          String msg = MsgUtils.getMsg("JOBS_QUEUE_INVALID_NAME_PREFIX", prefix);
          throw new JobQueueException(msg);
      }
      
      // Double check the priority of the default queue.
      String defaultQueue = JobQueueManagerNames.getDefaultQueue();
      if (defaultQueue.equals(queue.getName()) && 
          (DEFAULT_TENANT_QUEUE_PRIORITY != queue.getPriority())) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_INVALID_DEFAULT_QUEUE_DEF", 
                                     defaultQueue, DEFAULT_TENANT_QUEUE_PRIORITY);
        throw new JobQueuePriorityException(msg);
      }
      
      // Make sure no non-default queue has the default (lowest) priority.
      if (!defaultQueue.equals(queue.getName()) && 
          (DEFAULT_TENANT_QUEUE_PRIORITY == queue.getPriority())) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_INVALID_NON_DEFAULT_QUEUE_DEF", 
                                     queue.getName(), DEFAULT_TENANT_QUEUE_PRIORITY);
        throw new JobQueuePriorityException(msg);
      }
      
      // Validate the filter.
      validateFilter(queue.getFilter());
  }
  
  /* ---------------------------------------------------------------------- */
  /* validateFilter:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Attempt to parse the (non-empty) filter.  Parser errors are contained
   * in the thrown exception if things go wrong.
   * 
   * @param filter the text of a SQL92-like filter.
   */
  private void validateFilter(String filter)
   throws JobQueueFilterException
  {
      // Make sure 
      if (StringUtils.isBlank(filter)) {
          String msg = MsgUtils.getMsg("JOBS_QUEUE_FILTER_EMPTY");
          _log.error(msg);
          throw new JobQueueFilterException(msg);
      }
      
      // Try to parse the filter.
      SelectorFilter.parse(filter);
  }
}

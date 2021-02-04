package edu.utexas.tacc.tapis.jobs.dao;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobBlocked;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverConditionCode;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTesterType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public final class JobRecoveryDao 
 extends AbstractDao
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobRecoveryDao.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Assigned on first use.
    private JobBlockedDao _jobBlockedDao;
    
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
    public JobRecoveryDao() throws TapisException {}
      
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addJobRecovery:                                                        */
    /* ---------------------------------------------------------------------- */
    /** This method should only be called when all the blocked jobs contained
     * in the recovery object are in the BLOCKED status.  This method does not
     * update the jobs table, so in addition to the proper status value, the 
     * job record should have the blocked message in its last message field.
     * 
     * @param jobRecovery a complete recovery object
     * @throws JobException on error
     */
    public void addJobRecovery(JobRecovery jobRecovery) 
     throws TapisException
    {
        // ------------------------- Check Input -------------------------
        // Make sure we have a well-formed recovery object.
        try {jobRecovery.validate();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INPUT_ERROR", e.getMessage());
                throw new JobException(msg, e);
            }
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // See if there's already a recovery record for this condition.
          JobRecovery existingRecovery = getRecovery(conn, jobRecovery.getTenantId(), 
                                                     jobRecovery.getTesterHash());
          
          // Initialize values that get persisted.
          Instant now = Instant.now();
          int recoveryId = -1;
          
          // Create or update the recovery record and ALWAYS set the job recovery id.
          if (existingRecovery == null) recoveryId = createRecovery(conn, jobRecovery);
            else {
                recoveryId = existingRecovery.getId();
                updateTimestamp(conn, recoveryId, now, jobRecovery.getTenantId());
            }
          jobRecovery.setId(recoveryId); // the link between blocked jobs and their recovery record
          
          // Firewall against incomplete recovery objects.  We make sure there is NO WAY that
          // a recovery job object can be placed on the internal queue without a valid id.
          if (recoveryId <= 0) {
              String msg = MsgUtils.getMsg("JOBS_BAD_RECOVERY_ID_ASSIGNMENT", jobRecovery.getTenantId(), 
                                           jobRecovery.getConditionCode().name(), 
                                           jobRecovery.getTesterHash(), recoveryId, 
                                           (existingRecovery == null ? "NEW" : "EXISTING"));
              throw new JobException(msg);
          }
          
          // Process each job's information.  Validation guarantees 
          // that there's at least one blocked record.
          ListIterator<JobBlocked> it = jobRecovery.getBlockedJobs().listIterator();
          while (it.hasNext()) {
              // Get the next candidate job.
              JobBlocked blocked = it.next();
          
              // Fill in blocked record fields.  The create time
              // is set to be at least the time of the recovery
              // record's last update time.
              blocked.setRecoveryId(recoveryId);
              blocked.setCreated(now); 
              
              // Add the blocked record.  If a duplicate was found the call still succeeds. 
              getJobBlockedDao().addBlockedJob(conn, blocked);
          }
          
          // Commit all records as long as at least one job was blocked.  
          conn.commit();
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_CREATE_ERROR", jobRecovery.getTesterType().name(), 
                                         jobRecovery.getBlockedJobs().get(0).getJobUuid(), e.getMessage());
            throw JobUtils.tapisify(e, msg);
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
    /* deleteJobRecovery:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Delete the recovery job and all blocked job records associated with it.
     * A foreign key automatically cascades the deletion to the blocked jobs 
     * table.
     * 
     * @param recovery
     * @throws JobException 
     */
    public void deleteJobRecovery(long recoveryId, String tenantId) 
     throws JobException
    {
        // ------------------------- Tracing -----------------------------
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DELETING_RECOVERY_RECORD", recoveryId, tenantId);
            _log.debug(msg);
        }

        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Create the command using table definition field order.
          String sql = SqlStatements.DELETE_RECOVERY;
  
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setLong(1, recoveryId);
          pstmt.setString(2, tenantId);
      
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
            
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DELETE_ERROR", recoveryId, tenantId, e.getMessage());
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
    /* getRecoveryJobs:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Get all blocked jobs for the current tenant ordered by descending next 
     * attempt time (the soonest time first).  The returned list may be empty,
     * but never null.  Each object contains all the job blocked objects 
     * associated with the recovery object.
     * 
     * @return the non-null list of job recovery objects
     * @throws JobException 
     */
    public List<JobRecovery> getRecoveryJobs(String tenantId) 
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        // Get the tenant id to guard against inadvertent updates.
        if (StringUtils.isBlank(tenantId)) {
            String msg =  MsgUtils.getMsg("JOBS_NO_TENANT_ID");
            throw new JobException(msg);
        }
        
        // ------------------------- Call SQL ----------------------------
        ArrayList<JobRecovery> list = new ArrayList<>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try
        {
          // Get a database connection.
          conn = getConnection();
          
          // Create the command using table definition field order
          // in each row and descending next attempt row order.
          String sql = SqlStatements.SELECT_RECOVERY_BY_TENANT;
  
          // Prepare the statement and fill in the placeholders.
          pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenantId);
          
          // Execute the query.
          rs = pstmt.executeQuery();
          populateRecoveryJobs(list, rs);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_SELECT_ALL_ERROR", tenantId, e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            
            // Clean up db resources.
            if (rs != null) try {rs.close();} catch (Exception e) {}
            if (pstmt != null) try {pstmt.close();} catch (Exception e) {}

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
    /* updateAttempts:                                                        */
    /* ---------------------------------------------------------------------- */
    public void updateAttempts(JobRecovery jobRecovery) 
     throws JobException
    {
        // Update the recovery record timestamp.
        Connection conn = null;
        try {
            // Get a database connection.
            conn = getConnection();

            // Get the update timestamp.
            Instant now = Instant.now();
            
            // Create the command using table definition field order.
            String sql = SqlStatements.UPDATE_RECOVERY_ATTEMPTS;
    
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setTimestamp(1, Timestamp.from(now));
            pstmt.setInt(2, jobRecovery.getNumAttempts());
            pstmt.setTimestamp(3, Timestamp.from(jobRecovery.getNextAttempt()));
            pstmt.setLong(4, jobRecovery.getId());
            pstmt.setString(5, jobRecovery.getTenantId());
        
            // Issue the call.
            int rows = pstmt.executeUpdate();
            if (rows != 1) {
                String parms = StringUtils.joinWith(", ", now, jobRecovery.getNumAttempts(), 
                                                    jobRecovery.getNextAttempt(), jobRecovery.getId(), 
                                                    jobRecovery.getTenantId());
                String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
                throw new JobException(msg);
            }
            
            // Release resources.
            pstmt.close();
            
            // Commit everything.
            conn.commit();
            
            // Assign the job recovery last updated field.
            jobRecovery.setLastUpdated(now);
        }
        catch (Exception e) {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_UPDATE_ATTEMPTS", jobRecovery.getId(),
                                         jobRecovery.getNumAttempts(), jobRecovery.getNextAttempt(), 
                                         e.getMessage());
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
    /* getJobBlockedDao:                                                      */
    /* ---------------------------------------------------------------------- */
    private JobBlockedDao getJobBlockedDao() throws TapisException
    {
        if (_jobBlockedDao == null) _jobBlockedDao = new JobBlockedDao();
        return _jobBlockedDao;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRecovery:                                                           */
    /* ---------------------------------------------------------------------- */
    private JobRecovery getRecovery(Connection conn, String tenantId, String testerHash) 
     throws SQLException, TapisJDBCException
    {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            // Create the command using table definition field order.
            String sql = SqlStatements.SELECT_RECOVERY_BY_HASH_FOR_UPDATE;
    
            // Prepare the statement and fill in the placeholders.
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tenantId);
            pstmt.setString(2, testerHash);
        
            // Issue the call for the 0 or 1 row result set.
            rs = pstmt.executeQuery();
            return populateRecovery(rs);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_SELECT_ERROR", tenantId, 
                                         testerHash, e.getMessage());
            _log.error(msg, e);
            throw e;
        }
        finally {
            // Clean up db resources.
            if (rs != null) try {rs.close();} catch (Exception e) {}
            if (pstmt != null) try {pstmt.close();} catch (Exception e) {}
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* createRecovery:                                                        */
    /* ---------------------------------------------------------------------- */
    private int createRecovery(Connection conn, JobRecovery newRecovery) 
     throws SQLException, JobException
    {
        int newRecoveryId = -1;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            // Create the command using table definition field order.
            String sql = SqlStatements.CREATE_RECOVERY;
    
            // Prepare the statement and fill in the placeholders.
            // The fields that the DB defaults are not set and we
            // set the flag to retrieve the auto-generated id.
            pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, newRecovery.getTenantId());
            pstmt.setString(2, newRecovery.getConditionCode().name());
            pstmt.setString(3, newRecovery.getTesterType().name());
            pstmt.setString(4, TapisGsonUtils.getGson().toJson(newRecovery.getTesterParameters()));
            pstmt.setString(5, newRecovery.getPolicyType().name());
            pstmt.setString(6, TapisGsonUtils.getGson().toJson(newRecovery.getPolicyParameters()));
            pstmt.setInt(7, newRecovery.getNumAttempts());
            pstmt.setTimestamp(8, Timestamp.from(newRecovery.getNextAttempt()));
            pstmt.setTimestamp(9, Timestamp.from(newRecovery.getCreated()));
            pstmt.setTimestamp(10, Timestamp.from(newRecovery.getLastUpdated()));
            pstmt.setString(11, newRecovery.getTesterHash());
        
            // Issue the call.
            int rows = pstmt.executeUpdate();
            
            // Get the id that was just generated for the new record.
            rs = pstmt.getGeneratedKeys();
            
            // Get last generated sequence number on this connection
            if (rs.next()) newRecoveryId = rs.getInt(1);
             else {
                 // We were not able to retrieve the id generated for the new recovery record.
                 String msg = MsgUtils.getMsg("JOBS_RECOVERY_ID_ERROR", newRecovery.getTenantId(), 
                                              newRecovery.getConditionCode().name(), 
                                              newRecovery.getTesterHash());
                 throw new JobException(msg);
             }
            
            // Double check that we got a valid id. If this fails, we don't have the 
            // new record's id, so we don't have a way to reference it. Big problem. 
            if (newRecoveryId <= 0) {
                String msg = MsgUtils.getMsg("JOBS_BAD_RECOVERY_ID", newRecovery.getTenantId(), 
                                             newRecovery.getConditionCode().name(), 
                                             newRecovery.getTesterHash(), newRecoveryId);
                throw new JobException(msg);
            }
            
            // Always log the new record's auto-generated id.
            _log.info(MsgUtils.getMsg("JOBS_RECOVERY_ID_GENERATED", newRecovery.getTenantId(), 
                                      newRecovery.getConditionCode().name(), 
                                      newRecovery.getTesterHash(), newRecoveryId));
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_INSERT_ERROR", newRecovery.getTenantId(), 
                                         newRecovery.getConditionCode().name(), e.getMessage());
            _log.error(msg, e);
            throw e;
        }
        finally {
            // Clean up db resources.
            if (rs != null) try {rs.close();} catch (Exception e) {}
            if (pstmt != null) try {pstmt.close();} catch (Exception e) {}
        }
        
        return newRecoveryId;
    }

    /* ---------------------------------------------------------------------- */
    /* updateTimestamp:                                                       */
    /* ---------------------------------------------------------------------- */
    private void updateTimestamp(Connection conn, long id, Instant now, String tenantId) 
     throws SQLException, JobException
    {
        // Update the recovery record timestamp.
        PreparedStatement pstmt = null;
        try {
            // Create the command using table definition field order.
            String sql = SqlStatements.UPDATE_RECOVERY_TIMESTAMP;
    
            // Prepare the statement and fill in the placeholders.
            pstmt = conn.prepareStatement(sql);
            pstmt.setTimestamp(1, Timestamp.from(now));
            pstmt.setLong(2, id);
            pstmt.setString(3, tenantId);
        
            // Issue the call.
            int rows = pstmt.executeUpdate();
            if (rows != 1) {
                String parms = StringUtils.joinWith(", ", now, id, tenantId);
                String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
                throw new JobException(msg);
            }
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_UPDATE_ERROR", id, 
                                         e.getMessage());
            _log.error(msg, e);
            throw e;
        }
        finally {
            // Clean up db resources.
            if (pstmt != null) try {pstmt.close();} catch (Exception e) {}
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* populateRecovery:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Instantiate and populate a job recovery object with the results from a 
     * single row from the job_recovery table.
     * 
     * @param rs the result set for zeror or one job recovery record
     * @return the new, fully populated recovery object or null if the result set is empty 
     * @throws TapisJDBCException on error
     */
    private JobRecovery populateRecovery(ResultSet rs) 
     throws TapisJDBCException
    {
        // Quick check.
        if (rs == null) return null;
        
        // Return null if the results are empty or exhausted.
        // This call advances the cursor.
        JobRecovery rec = null;
        try {
            // It's ok if no row was returned.
            if (!rs.next()) return null;
            
            // Fill in the recovery record fields.
            rec = new JobRecovery();
            populateRecoveryFields(rec, rs);
        }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
          throw new TapisJDBCException(msg, e);
        }
        
        return rec;
    }
    
    /* ---------------------------------------------------------------------- */
    /* populateRecoveryJobs:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Instantiate and populate a job recovery object for each row returned in
     * the result set.  The recovery objects are added in result set order to 
     * provided list.
     * 
     * @param rs the result set for zero or more job recovery records
     * @throws TapisJDBCException on error
     */
    private void populateRecoveryJobs(ArrayList<JobRecovery> list, ResultSet rs) 
     throws TapisJDBCException
    {
        // Quick check.
        if (rs == null) return;
        
        // Return null if the results are empty or exhausted.
        // This call advances the cursor.
        JobRecovery rec = null;
        try {
            // It's ok if no row was returned.
            while (rs.next()) {
            
                // Fill in the recovery record fields for current row.
                rec = new JobRecovery();
                populateRecoveryFields(rec, rs);
                list.add(rec);
            }
        }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
          throw new TapisJDBCException(msg, e);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* populateRecoveryFields:                                                */
    /* ---------------------------------------------------------------------- */
    /** Assign job recovery fields from a propositioned result set.  This method
     * does not change the state of the result set or perform any clean up.
     * 
     * @param rec the object that gets assigned 
     * @param rs the non-null result set
     * @throws SQLException on error
     */
    private void populateRecoveryFields(JobRecovery rec, ResultSet rs) 
     throws SQLException
    {
        // Used to reconstitute sorted maps from json.
        Type treeMapType = new TypeToken<TreeMap<String,String>>(){}.getType();
        
        // Start filling in fields.
        rec.setId(rs.getInt(1));
        rec.setTenantId(rs.getString(2));
        rec.setConditionCode(RecoverConditionCode.valueOf(rs.getString(3)));
        rec.setTesterType(RecoverTesterType.valueOf(rs.getString(4)));
        rec.setTesterParameters(TapisGsonUtils.getGson().fromJson(rs.getString(5), treeMapType));
        rec.setPolicyType(RecoverPolicyType.valueOf(rs.getString(6)));
        rec.setPolicyParameters(TapisGsonUtils.getGson().fromJson(rs.getString(7), treeMapType));
        rec.setNumAttempts(rs.getInt(8));
        
        // Timestamp fields should not be null, but we do some future-proofing.
        Timestamp ts = rs.getTimestamp(9);
        if (ts != null) rec.setNextAttempt(ts.toInstant());
        ts = rs.getTimestamp(10);
        if (ts != null) rec.setCreated(ts.toInstant());
        ts = rs.getTimestamp(11);
        if (ts != null) rec.setLastUpdated(ts.toInstant());
        
        // The sha1 hash is always 40 hex characters (160 bits).
        rec.setTesterHash(rs.getString(12));
    }

}

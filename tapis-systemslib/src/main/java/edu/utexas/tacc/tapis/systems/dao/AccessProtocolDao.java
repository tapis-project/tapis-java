package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.model.System;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/*
 * Class to handle persistence for AccessProtocol objects.
 */
public class AccessProtocolDao extends AbstractDao
{
  /* ********************************************************************** */
  /*                               Fields                                   */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(AccessProtocolDao.class);

//    private final DataSource dataSource;

  /* ********************************************************************** */
  /*                             Constructors                               */
  /* ********************************************************************** */
//  @Inject
//  AccessProtocolDao(DataSource dataSource1) {
//    dataSource = dataSource1;
//  }

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* createSystem:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Insert a new record.
   *
   * @param mechanism
   * @throws TapisException on error
   */
  public void createAccessProtocol(String mechanism, int port, boolean useProxy, String proxyHost, int proxyPort)
    throws TapisException
  {
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(mechanism)) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "mechanism");
      _log.error(msg);
      throw new TapisException(msg);
    }
        
  // ------------------------- Call SQL ----------------------------
  Connection conn = null;
  try
  {
    // Get a database connection.
    conn = getConnection();

    // Set the sql command.
    String sql = SqlStatements.CREATE_SYSTEM;

    // Prepare the statement and fill in the placeholders.
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setString(1, mechanism);
    pstmt.setInt(2, port);
    pstmt.setBoolean(3, true);
    pstmt.setString(4, proxyHost);
    pstmt.setInt(5, proxyPort);

    // Issue the call.
    int rows = pstmt.executeUpdate();
    if (rows != 1) {
      String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, mechanism);
      _log.error(msg);
      throw new TapisException(msg);
    }

    // Commit the transaction.
    conn.commit();
  }
  catch (Exception e)
  {
    // Rollback transaction.
    try {if (conn != null) conn.rollback();}
    catch (Exception e1)
    {
      _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
      // Log the exception.
      String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems_tbl");
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Conditionally return the connection back to the connection pool.
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e) {
          // If commit worked, we can swallow the exception.
          // If not, the commit exception will be thrown.
          String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
          _log.error(msg, e);
        }
      }
    }
  }
    
    /* ---------------------------------------------------------------------- */
    /* getSystemById:                                                         */
    /* ---------------------------------------------------------------------- */
    public System getSystemByName(String name)
     throws TapisException
    {
        // Initialize result.
        System result = null;

        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
            conn = getConnection();
            
            // Get the select command.
            String sql = SqlStatements.SELECT_SYSTEM_BY_ID;
            
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
                        
            // Issue the call for the 1 row result set.
            ResultSet rs = pstmt.executeQuery();
            result = populateSystem(rs);
            
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
                catch (Exception e1){
                  _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "System", name, e.getMessage());
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
    /* getSystems:                                                            */
    /* ---------------------------------------------------------------------- */
    public List<System> getSystems() 
     throws TapisException
    {
        // The result list is always non-null.
        var list = new ArrayList<System>();
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
            conn = getConnection();
            
            // Get the select command.
            String sql = SqlStatements.SELECT_ALL_SYSTEMS;
            
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
                        
            // Issue the call for the 1 row result set.
            ResultSet rs = pstmt.executeQuery();
            System system = populateSystem(rs);
            while (system != null) {
                list.add(system);
                system = populateSystem(rs);
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
                catch (Exception e1){
                  _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("DB_QUERY_ERROR", "samples", e.getMessage());
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
    /* populateSystem:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Instantiate and populate a sample object with the results from a single
     * row from the sample_tbl table.
     * 
     * @param rs the result set for one job
     * @return the new, fully populated job object or null if the result set is empty 
     * @throws TapisJDBCException 
     */
    private System populateSystem(ResultSet rs) 
     throws TapisJDBCException
    {
      return null;
      /*
        // Quick check.
        if (rs == null) return null;
        
        // Return null if the results are empty or exhausted.
        // This call advances the cursor.
        try {if (!rs.next()) return null;}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
          log.error(msg, e);
          throw new TapisJDBCException(msg, e);
        }
        
        // Create the mostly empty object and then overwrite all fields.
        System system = new System();
        try {
            system.setId(rs.getInt(1));
            system.setName(rs.getString(2));
            system.setUpdated(rs.getTimestamp(3).toInstant());
        } 
          catch (Exception e) {
            String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
            log.error(msg, e);
            throw new TapisJDBCException(msg, e);
          }
        return system;
       */
    }
}

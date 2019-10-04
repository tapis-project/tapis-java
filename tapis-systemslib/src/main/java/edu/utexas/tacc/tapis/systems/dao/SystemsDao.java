package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Class to handle persistence for System objects.
 */
public class SystemsDao extends AbstractDao
{
  // TODO Remove hard coded value
  private static final String tenant = "tenant1";

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsDao.class);

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /**
   * Create a new system record.
   *
   */
  public int createSystem(String tenant, String name, String description, String owner, String host,
                           boolean available, String bucketName, String rootDir, String effectiveUserId)
          throws TapisException
  {
    int rows = -1;
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(tenant)) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "tenant");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (StringUtils.isBlank(name)) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "name");
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
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      pstmt.setString(3, description);
      pstmt.setString(4, owner);
      pstmt.setString(5, host);
      pstmt.setBoolean(6, available);
      pstmt.setString(7, bucketName);
      pstmt.setString(8, rootDir);
      pstmt.setString(9, effectiveUserId);

      // Issue the call.
      rows = pstmt.executeUpdate();
      if (rows != 1)
      {
        String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, name);
        _log.error(msg);
        throw new TapisException(msg);
      }

      // Close out and commit
      pstmt.close();
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      // Log the exception.
      String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems_tbl");
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Conditionally return the connection back to the connection pool.
      if (conn != null)
        try
        {
          conn.close();
        }
        catch (Exception e)
        {
          // If commit worked, we can swallow the exception.
          // If not, the commit exception will be thrown.
          String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
          _log.error(msg, e);
        }
    }
    return rows;
  }

  /**
   * Delete a system record given the system name.
   *
   */
  public int deleteSystem(String tenant, String name)
      throws TapisException
  {
    int rows = -1;
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(tenant)) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "tenant");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (StringUtils.isBlank(name))
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteSystem", "name");
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
      String sql = SqlStatements.DELETE_SYSTEM_BY_NAME;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);

      // Issue the call.
      rows = pstmt.executeUpdate();

      // Close out and commit
      pstmt.close();
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      // Log the exception.
      String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems_tbl");
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Conditionally return the connection back to the connection pool.
      if (conn != null)
        try
        {
          conn.close();
        }
        catch (Exception e)
        {
          // If commit worked, we can swallow the exception.
          // If not, the commit exception will be thrown.
          String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
          _log.error(msg, e);
        }
    }
    return rows;
  }

  /**
   * getSystemByName
   * @param name
   * @return
   * @throws TapisException
   */
  public TSystem getSystemByName(String name) throws TapisException {
    // Initialize result.
    TSystem result = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_SYSTEM_BY_NAME;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);

      // Issue the call for the 1 row result set.
      ResultSet rs = pstmt.executeQuery();
      result = populateSystem(rs);

      // Close out and commit
      rs.close();
      pstmt.close();
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      String msg = MsgUtils.getMsg("DB_SELECT_NAME_ERROR", "System", tenant, name, e.getMessage());
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      try
      {
        if (conn != null) conn.close();
      }
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
  public List<TSystem> getSystems()
          throws TapisException
  {
    // The result list is always non-null.
    var list = new ArrayList<TSystem>();

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
      TSystem system = populateSystem(rs);
      while (system != null)
      {
        list.add(system);
        system = populateSystem(rs);
      }

      // Close out and commit
      rs.close();
      pstmt.close();
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      String msg = MsgUtils.getMsg("DB_QUERY_ERROR", "samples", e.getMessage());
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      try
      {
        if (conn != null) conn.close();
      }
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

  /**
   * populateSystem
   * Instantiate and populate an object using single row result
   *
   * @param rs the result set for one job
   * @return the new, fully populated job object or null if the result set is empty
   * @throws TapisJDBCException
   */
  private TSystem populateSystem(ResultSet rs)
          throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;

    // Return null if the results are empty or exhausted.
    // This call advances the cursor.
    try { if (!rs.next()) return null; }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }

    TSystem system = null;
    try
    {
      system = new TSystem(rs.getInt(1),
                           rs.getString(2),
                           rs.getString(3),
                           rs.getString(4),
                           rs.getString(5),
                           rs.getString(6),
                           rs.getBoolean(7),
                           rs.getString(8),
                           rs.getString(9),
                           rs.getString(10),
                           rs.getTimestamp(11).toInstant(),
                           rs.getTimestamp(12).toInstant());
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    return system;
  }
}

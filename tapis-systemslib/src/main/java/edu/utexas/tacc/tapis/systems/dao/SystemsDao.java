package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.model.System;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Class to handle persistence for System objects.
 */
public class SystemsDao extends AbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(SystemsDao.class);

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* createSystem:                                                          */
  /* ---------------------------------------------------------------------- */

  /**
   * Insert a new system record.
   *
   */
  public void create(String tenant, String name, String description, String owner, String host,
                     boolean available, String bucketName, String rootDir, String effectiveUserId)
          throws TapisException
  {
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(name))
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "name");
      log.error(msg);
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
      int rows = pstmt.executeUpdate();
      if (rows != 1)
      {
        String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, name);
        log.error(msg);
        throw new TapisException(msg);
      }

      // Commit the transaction.
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
        log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      // Log the exception.
      String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems_tbl");
      log.error(msg, e);
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
          log.error(msg, e);
        }
    }
  }

  /* ---------------------------------------------------------------------- */
  /* getSystemById:                                                         */
  /* ---------------------------------------------------------------------- */
  public System getSystemById(int id)
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
      pstmt.setInt(1, id);

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
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "System", id, e.getMessage());
      log.error(msg, e);
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
        log.error(msg, e);
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
      while (system != null)
      {
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
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      String msg = MsgUtils.getMsg("DB_QUERY_ERROR", "samples", e.getMessage());
      log.error(msg, e);
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
        log.error(msg, e);
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
  private System populateSystem(ResultSet rs)
          throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;

    // Return null if the results are empty or exhausted.
    // This call advances the cursor.
    try
    {
      if (!rs.next()) return null;
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
      log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }

    System system = null;
    try
    {
      system = new System(rs.getInt(1),
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
      log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    return system;
  }
}

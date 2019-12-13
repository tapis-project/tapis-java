package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMechanism;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Class to handle persistence for Tapis System objects.
 */
@Singleton
public class SystemsDaoImpl extends AbstractDao implements SystemsDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsDaoImpl.class);

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /**
   * Create a new system object.
   *
   * @return Sequence id of object created
   * @throws TapisException
   */
  @Override
  public int createTSystem(String tenant, String name, String description, String owner, String host,
                           boolean available, String bucketName, String rootDir,
                           String jobInputDir, String jobOutputDir, String workDir, String scratchDir,
                           String effectiveUserId, String tags, String notes, String accessMechanism, String transferMechanisms,
                           int port, boolean useProxy, String proxyHost, int proxyPort, String rawRequest)
          throws TapisException, IllegalStateException
  {
    // Generated sequence id
    int itemId;
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
    if (StringUtils.isBlank(accessMechanism))
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "accessMechanism");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (transferMechanisms == null || StringUtils.isBlank(transferMechanisms)) transferMechanisms = Protocol.DEFAULT_TRANSFER_MECHANISMS_STR;

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    if (proxyHost == null) proxyHost = Protocol.DEFAULT_PROXYHOST;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Check to see if system exists. If yes then throw IllegalStateException
      String sql = SqlStatements.CHECK_FOR_SYSTEM_BY_NAME;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      // Should get one row back. If not assume system does not exist
      boolean doesExist = false;
      ResultSet rs = pstmt.executeQuery();
      if (rs != null && rs.next()) doesExist = rs.getBoolean(1);
      if (doesExist) throw new IllegalStateException(LibUtils.getMsg("SYSLIB_SYS_EXISTS", name));

      // Set the sql command.
      sql = SqlStatements.CREATE_SYSTEM;

      // Convert tags and notes to jsonb objects
      var tagsJson = new PGobject();
      tagsJson.setType("jsonb");
      tagsJson.setValue(tags);
      var notesJson = new PGobject();
      notesJson.setType("jsonb");
      notesJson.setValue(notes);

      // Prepare the statement and fill in the placeholders.
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      pstmt.setString(3, description);
      pstmt.setString(4, owner);
      pstmt.setString(5, host);
      pstmt.setBoolean(6, available);
      pstmt.setString(7, bucketName);
      pstmt.setString(8, rootDir);
      pstmt.setString(9, jobInputDir);
      pstmt.setString(10, jobOutputDir);
      pstmt.setString(11, workDir);
      pstmt.setString(12, scratchDir);
      pstmt.setString(13, effectiveUserId);
      pstmt.setObject(14, tagsJson);
      pstmt.setObject(15, notesJson);
      pstmt.setString(16, accessMechanism);
      pstmt.setString(17, transferMechanisms);
      pstmt.setInt(18, port);
      pstmt.setBoolean(19, useProxy);
      pstmt.setString(20, proxyHost);
      pstmt.setInt(21, proxyPort);
      pstmt.setString(22, rawRequest);

      // Issue the call.
      pstmt.execute();
      // The generated sequence id should come back in the doesExist
      rs = pstmt.getResultSet();
      if (!rs.next())
      {
        String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems");
        _log.error(msg);
        throw new TapisException(msg);
      }
      itemId = rs.getInt(1);

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

      // If IllegalStateException pass it back up
      if (e instanceof IllegalStateException) throw (IllegalStateException) e;

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
    return itemId;
  }

  /**
   * Delete a system record given the system name.
   *
   */
  @Override
  public int deleteTSystem(String tenant, String name) throws TapisException
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
      String msg = MsgUtils.getMsg("DB_DELETE_FAILURE", "systems");
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
   * checkForSystemByName
   * @param name
   * @return true if found else false
   * @throws TapisException
   */
  @Override
  public boolean checkForTSystemByName(String tenant, String name) throws TapisException {
    // Initialize result.
    boolean result = false;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.CHECK_FOR_SYSTEM_BY_NAME;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);

      // Should get one row back. If not return null.
      ResultSet rs = pstmt.executeQuery();
      if (rs != null && rs.next()) result = rs.getBoolean(1);

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

  /**
   * getSystemByName
   * @param name
   * @return System object if found, null if not found
   * @throws TapisException
   */
  @Override
  public TSystem getTSystemByName(String tenant, String name) throws TapisException {
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

      // Should get one row back. If not return null.
      // Use result to populate system object
      ResultSet rs = pstmt.executeQuery();
      if (rs != null && rs.next()) result = populateTSystem(rs);

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

  /**
   * getSystems
   * @param tenant
   * @return
   * @throws TapisException
   */
  @Override
  public List<TSystem> getTSystems(String tenant) throws TapisException
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
      pstmt.setString(1, tenant);

      // Issue the call for the 1 row result set.
      ResultSet rs = pstmt.executeQuery();
      if (rs != null)
      {
        while (rs.next())
        {
          TSystem system = populateTSystem(rs);
          if (system != null) list.add(system);
        }
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

  /**
   * getSystemNames
   * @param tenant
   * @return
   * @throws TapisException
   */
  @Override
  public List<String> getTSystemNames(String tenant) throws TapisException
  {
    // The result list is always non-null.
    var list = new ArrayList<String>();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_ALL_SYSTEM_NAMES;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);

      // Issue the call for the 1 row result set.
      ResultSet rs = pstmt.executeQuery();
      while (rs.next()) list.add(rs.getString(1));

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

  /**
   * getSystemOwner
   * @param tenant
   * @param name - name of the system
   * @return Owner or null if no system found
   * @throws TapisException
   */
  @Override
  public String getTSystemOwner(String tenant, String name) throws TapisException
  {
    String owner = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_SYSTEM_OWNER;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);

      // Issue the call for the 1 row result set.
      ResultSet rs = pstmt.executeQuery();
      if (rs.next()) owner = rs.getString(1);

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
    return owner;
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /**
   * populateTSystem
   * Instantiate and populate an object using a result set. The cursor for the
   *   ResultSet must be advanced to the desired result.
   *
   * @param rs the result set containing one or more items, cursor at desired item
   * @return the new, fully populated job object or null if there is a problem
   * @throws TapisJDBCException
   */
  private TSystem populateTSystem(ResultSet rs) throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;

    TSystem tSystem = null;
    Protocol prot = null;
    try
    {
      // TODO: What about credentials?

      // Populate protocol transfer mechanisms
      List<TransferMechanism> tmechsList = new ArrayList<>();
      String tmechsStr = rs.getString(18);
      if (tmechsStr != null && !StringUtils.isBlank(tmechsStr))
      {
        // Strip off surrounding braces and convert strings to enums
        // NOTE: All values should be valid due to enforcement of type in DB and json schema validation
        String[] tmechsStrArray = (tmechsStr.substring(1, tmechsStr.length() - 1)).split(",");
        for (String tmech : tmechsStrArray)
        {
          if (!StringUtils.isBlank(tmech)) tmechsList.add(Protocol.TransferMechanism.valueOf(tmech));
        }
      }

      tSystem = new TSystem(rs.getInt(1), // id
                            rs.getString(2), // tenant
                            rs.getString(3), // name
                            rs.getString(4), // description
                            rs.getString(5), // owner
                            rs.getString(6), // host
                            rs.getBoolean(7), //available
                            rs.getString(8), // bucketName
                            rs.getString(9), // rootDir
                            rs.getString(10), // jobInputDir
                            rs.getString(11), // jobOutputDir
                            rs.getString(12), // workDir
                            rs.getString(13), // scratchDir
                            rs.getString(14), // effectiveUserId
                            rs.getString(15), // tags
                            rs.getString(16), // notes
                            AccessMechanism.valueOf(rs.getString(17)),
                            tmechsList,
                            rs.getInt(19),
                            rs.getBoolean(20),
                            rs.getString(21),
                            rs.getInt(22),
                           "fakeAccessCred1".toCharArray(), // accessCred
                            rs.getTimestamp(23).toInstant(), // created
                            rs.getTimestamp(24).toInstant()); // updated
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    return tSystem;
  }
}

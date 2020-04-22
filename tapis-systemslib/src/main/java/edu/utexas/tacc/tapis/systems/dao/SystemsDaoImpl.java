package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Capability.Category;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Class to handle persistence for Tapis System objects.
 */
public class SystemsDaoImpl extends AbstractDao implements SystemsDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsDaoImpl.class);

  private static final String EMPTY_JSON = "{}";

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /**
   * Create a new system.
   *
   * @return Sequence id of object created
   * @throws TapisException - on error
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public int createTSystem(AuthenticatedUser authenticatedUser, TSystem system, String createJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException {
    String opName = "createSystem";
    // Generated sequence id
    int systemId = -1;
    // ------------------------- Check Input -------------------------
    if (system == null) LibUtils.logAndThrowNullParmException(opName, "system");
    if (authenticatedUser == null) LibUtils.logAndThrowNullParmException(opName, "authenticatedUser");
    if (StringUtils.isBlank(createJsonStr)) LibUtils.logAndThrowNullParmException(opName, "createJson");
    if (StringUtils.isBlank(system.getTenant())) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(system.getName())) LibUtils.logAndThrowNullParmException(opName, "systemName");
    if (system.getSystemType() == null) LibUtils.logAndThrowNullParmException(opName, "systemType");
    if (system.getDefaultAccessMethod() == null) LibUtils.logAndThrowNullParmException(opName, "defaultAccessMethod");

    // Convert transferMethods into a string
    String transferMethodsStr = LibUtils.getTransferMethodsAsString(system.getTransferMethods());

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    String proxyHost = TSystem.DEFAULT_PROXYHOST;
    if (system.getProxyHost() != null) proxyHost = system.getProxyHost();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Check to see if system exists or has been soft deleted. If yes then throw IllegalStateException
      boolean doesExist = checkForTSystem(conn, system.getTenant(), system.getName(), true);
      if (doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", authenticatedUser, system.getName()));

      // Make sure owner, effectiveUserId, notes and tags are all set
      String owner = TSystem.DEFAULT_OWNER;
      if (StringUtils.isNotBlank(system.getOwner())) owner = system.getOwner();
      String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
      if (StringUtils.isNotBlank(system.getEffectiveUserId())) effectiveUserId = system.getEffectiveUserId();
      String tagsStr = TSystem.DEFAULT_TAGS_STR;
      if (system.getTags() != null) tagsStr = TapisGsonUtils.getGson().toJson(system.getTags());
      JsonObject notesObj = TSystem.DEFAULT_NOTES;
      if (system.getNotes() != null) notesObj = (JsonObject) system.getNotes();

      // Convert tags and notes to jsonb objects.
      // Tags is a list of strings and notes is a JsonObject
      var tagsJsonb = new PGobject();
      tagsJsonb.setType("jsonb");
      tagsJsonb.setValue(tagsStr);
      var notesJsonb = new PGobject();
      notesJsonb.setType("jsonb");
      notesJsonb.setValue(notesObj.toString());

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.CREATE_SYSTEM;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, system.getTenant());
      pstmt.setString(2, system.getName());
      pstmt.setString(3, system.getDescription());
      pstmt.setString(4, system.getSystemType().name());
      pstmt.setString(5, owner);
      pstmt.setString(6, system.getHost());
      pstmt.setBoolean(7, system.isEnabled());
      pstmt.setString(8, effectiveUserId);
      pstmt.setString(9, system.getDefaultAccessMethod().name());
      pstmt.setString(10, system.getBucketName());
      pstmt.setString(11, system.getRootDir());
      pstmt.setString(12, transferMethodsStr);
      pstmt.setInt(13, system.getPort());
      pstmt.setBoolean(14, system.isUseProxy());
      pstmt.setString(15, proxyHost);
      pstmt.setInt(16, system.getProxyPort());
      pstmt.setBoolean(17, system.getJobCanExec());
      pstmt.setString(18, system.getJobLocalWorkingDir());
      pstmt.setString(19, system.getJobLocalArchiveDir());
      pstmt.setString(20, system.getJobRemoteArchiveSystem());
      pstmt.setString(21, system.getJobRemoteArchiveDir());
      pstmt.setObject(22, tagsJsonb);
      pstmt.setObject(23, notesJsonb);
      pstmt.execute();
      // The generated sequence id should come back as result
      ResultSet rs = pstmt.getResultSet();
      if (rs == null || !rs.next())
      {
        String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems");
        _log.error(msg);
        throw new TapisException(msg);
      }
      systemId = rs.getInt(1);
      pstmt.close();
      rs.close();
      rs = null;
      pstmt = null;

      // Persist job capabilities
      persistJobCapabilities(conn, system, systemId);

      // Persist update record
      addUpdate(conn, systemId, SystemOperation.create.name(), createJsonStr, scrubbedText);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return systemId;
  }

  /**
   * Update an existing system.
   * Following attributes will be updated:
   *  description, host, enabled, effectiveUserId, defaultAccessMethod, transferMethods,
   *  port, useProxy, proxyHost, proxyPort, jobCapabilities, tags, notes
   * @return Sequence id of object created
   * @throws TapisException - on error
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public int updateTSystem(AuthenticatedUser authenticatedUser, TSystem patchedSystem, PatchSystem patchSystem,
                           String updateJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException {
    String opName = "updateSystem";
    // ------------------------- Check Input -------------------------
    if (patchedSystem == null) LibUtils.logAndThrowNullParmException(opName, "patchedSystem");
    if (patchSystem == null) LibUtils.logAndThrowNullParmException(opName, "patchSystem");
    if (authenticatedUser == null) LibUtils.logAndThrowNullParmException(opName, "authenticatedUser");
    if (StringUtils.isBlank(updateJsonStr)) LibUtils.logAndThrowNullParmException(opName, "updateJson");
    if (StringUtils.isBlank(patchedSystem.getTenant())) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(patchedSystem.getName())) LibUtils.logAndThrowNullParmException(opName, "systemName");
    if (patchedSystem.getSystemType() == null) LibUtils.logAndThrowNullParmException(opName, "systemType");
    if (patchedSystem.getId() < 1) LibUtils.logAndThrowNullParmException(opName, "systemId");
    // Pull out some values for convenience
    String tenant = patchedSystem.getTenant();
    String name = patchedSystem.getName();
    int systemId = patchedSystem.getId();

    // Convert transferMethods into a string
    String transferMethodsStr = LibUtils.getTransferMethodsAsString(patchedSystem.getTransferMethods());

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    String proxyHost = TSystem.DEFAULT_PROXYHOST;
    if (patchedSystem.getProxyHost() != null) proxyHost = patchedSystem.getProxyHost();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Check to see if system exists and has not been soft deleted. If no then throw IllegalStateException
      boolean doesExist = checkForTSystem(conn, tenant, name, false);
      if (!doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, name));

      // Make sure effectiveUserId, notes and tags are all set
      String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
      if (StringUtils.isNotBlank(patchedSystem.getEffectiveUserId())) effectiveUserId = patchedSystem.getEffectiveUserId();
      String tagsStr = TSystem.DEFAULT_TAGS_STR;
      if (patchedSystem.getTags() != null) tagsStr = TapisGsonUtils.getGson().toJson(patchedSystem.getTags());
      JsonObject notesObj =  TSystem.DEFAULT_NOTES;
      if (patchedSystem.getNotes() != null) notesObj = (JsonObject) patchedSystem.getNotes();

      // Convert tags and notes to jsonb objects.
      // Tags is a list of strings and notes is a JsonObject
      var tagsJsonb = new PGobject();
      tagsJsonb.setType("jsonb");
      tagsJsonb.setValue(tagsStr);
      var notesJsonb = new PGobject();
      notesJsonb.setType("jsonb");
      notesJsonb.setValue(notesObj.toString());

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.UPDATE_SYSTEM;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, patchedSystem.getDescription());
      pstmt.setString(2, patchedSystem.getHost());
      pstmt.setBoolean(3, patchedSystem.isEnabled());
      pstmt.setString(4, effectiveUserId);
      pstmt.setString(5, patchedSystem.getDefaultAccessMethod().name());
      pstmt.setString(6, transferMethodsStr);
      pstmt.setInt(7, patchedSystem.getPort());
      pstmt.setBoolean(8, patchedSystem.isUseProxy());
      pstmt.setString(9, proxyHost);
      pstmt.setInt(10, patchedSystem.getProxyPort());
      pstmt.setObject(11, tagsJsonb);
      pstmt.setObject(12, notesJsonb);
      pstmt.setInt(13, systemId);
      pstmt.execute();

      // If jobCapabilities updated then replace them
      if (patchSystem.getJobCapabilities() != null) {
        removeJobCapabilities(conn, systemId);
        persistJobCapabilities(conn, patchedSystem, systemId);
      }

      // Persist update record
      addUpdate(conn, systemId, SystemOperation.modify.name(), updateJsonStr, scrubbedText);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return systemId;
  }

  /**
   * Update owner of a system given system Id and new owner name
   *
   */
  @Override
  public void updateSystemOwner(int systemId, String newOwnerName) throws TapisException
  {
    String opName = "changeOwner";
    // ------------------------- Check Input -------------------------
    if (systemId < 1) LibUtils.logAndThrowNullParmException(opName, "systemId");
    if (StringUtils.isBlank(newOwnerName)) LibUtils.logAndThrowNullParmException(opName, "newOwnerName");

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.UPDATE_SYSTEM_OWNER;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, newOwnerName);
      pstmt.setInt(2, systemId);
      pstmt.executeUpdate();
      // Persist update record
      String updateJsonStr = TapisGsonUtils.getGson().toJson(newOwnerName);
      addUpdate(conn, systemId, SystemOperation.changeOwner.name(), updateJsonStr , null);
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Soft delete a system record given the system name.
   *
   */
  @Override
  public int softDeleteTSystem(int systemId) throws TapisException
  {
    String opName = "softDeleteSystem";
    int rows = -1;
    // ------------------------- Check Input -------------------------
    if (systemId < 1) LibUtils.logAndThrowNullParmException(opName, "systemId");

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.SOFT_DELETE_SYSTEM_BY_ID;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, systemId);
      rows = pstmt.executeUpdate();

      // Persist update record
      addUpdate(conn, systemId, SystemOperation.softDelete.name(), EMPTY_JSON, null);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return rows;
  }

  /**
   * Hard delete a system record given the system name.
   *
   */
  @Override
  public int hardDeleteTSystem(String tenant, String name) throws TapisException
  {
    String opName = "hardDeleteSystem";
    int rows = -1;
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(tenant)) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(name)) LibUtils.logAndThrowNullParmException(opName, "name");

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.HARD_DELETE_SYSTEM_BY_NAME;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      rows = pstmt.executeUpdate();

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return rows;
  }

  /**
   * checkForSystemByName
   * @param name - system name
   * @return true if found else false
   * @throws TapisException - on error
   */
  @Override
  public boolean checkForTSystemByName(String tenant, String name, boolean includeDeleted) throws TapisException {
    // Initialize result.
    boolean result = false;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      // Run the sql
      result = checkForTSystem(conn, tenant, name, includeDeleted);
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_SELECT_NAME_ERROR", "System", tenant, name, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * getSystemByName
   * @param name - system name
   * @return System object if found, null if not found
   * @throws TapisException - on error
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

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.SELECT_SYSTEM_BY_NAME;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      ResultSet rsSys = pstmt.executeQuery();
      // Should get one row back. If not close out and return.
      if (rsSys == null || !rsSys.next())
      {
        LibUtils.closeAndCommitDB(conn, pstmt, rsSys);
        return null;
      }

      // Pull out the system id so we can use it to get capabilities
      int systemId = rsSys.getInt(1);

      // Retrieve job capabilities
      List<Capability> jobCaps = retrieveJobCaps(systemId, conn);

      // Use results to populate system object
      result = populateTSystem(rsSys, jobCaps);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rsSys);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_SELECT_NAME_ERROR", "System", tenant, name, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }

    return result;
  }

  /**
   * getSystems
   * @param tenant - tenant name
   * @return - list of TSystem objects
   * @throws TapisException - on error
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

      // Prepare the statement, fill in placeholders and execute
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      ResultSet rs = pstmt.executeQuery();
      // Iterate over results
      if (rs != null)
      {
        while (rs.next())
        {
          // Retrieve job capabilities
          int systemId = rs.getInt(1);
          List<Capability> jobCaps = retrieveJobCaps(systemId, conn);
          TSystem system = populateTSystem(rs, jobCaps);
          if (system != null) list.add(system);
        }
      }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }

    return list;
  }

  /**
   * getSystemNames
   * @param tenant - tenant name
   * @return - List of system names
   * @throws TapisException - on error
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

      // Prepare the statement, fill in placeholders and execute
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      ResultSet rs = pstmt.executeQuery();
      // Iterate over result
      while (rs.next()) list.add(rs.getString(1));

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }

    return list;
  }

  /**
   * getSystemOwner
   * @param tenant - name of tenant
   * @param name - name of system
   * @return Owner or null if no system found
   * @throws TapisException - on error
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

      // Prepare the statement, fill in placeholders and execute
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      ResultSet rs = pstmt.executeQuery();
      if (rs.next()) owner = rs.getString(1);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return owner;
  }

  /**
   * getSystemEffectiveUserId
   * @param tenant - name of tenant
   * @param name - name of system
   * @return EffectiveUserId or null if no system found
   * @throws TapisException - on error
   */
  @Override
  public String getTSystemEffectiveUserId(String tenant, String name) throws TapisException
  {
    String effectiveUserId = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_SYSTEM_EFFECTIVEUSERID;

      // Prepare the statement, fill in placeholders and execute
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      ResultSet rs = pstmt.executeQuery();
      if (rs.next()) effectiveUserId = rs.getString(1);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return effectiveUserId;
  }

  /**
   * getSystemId
   * @param tenant - name of tenant
   * @param name - name of system
   * @return systemId or -1 if no system found
   * @throws TapisException - on error
   */
  @Override
  public int getTSystemId(String tenant, String name) throws TapisException
  {
    int systemId = -1;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_SYSTEM_ID;

      // Prepare the statement, fill in placeholders and execute
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      ResultSet rs = pstmt.executeQuery();
      if (rs.next()) systemId = rs.getInt(1);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return systemId;
  }

  /**
   * Add an update record given the system Id and operation type
   *
   */
  @Override
  public void addUpdateRecord(int systemId, String opName, String upd_json, String upd_text) throws TapisException
  {
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      addUpdate(conn, systemId, opName, upd_json, upd_text);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /**
   * Given an sql connection and basic info add an update record
   *
   */
  private void addUpdate(Connection conn, int systemId, String opName, String upd_json, String upd_text)
          throws SQLException
  {
    String updJsonStr = (StringUtils.isBlank(upd_json)) ? EMPTY_JSON : upd_json;
    // Convert upd_json to jsonb object.
    var pGobject = new PGobject();
    pGobject.setType("jsonb");
    pGobject.setValue(updJsonStr);

    // Persist update record
    String sql = SqlStatements.ADD_UPDATE;
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setInt(1, systemId);
    pstmt.setString(2, opName);
    pstmt.setObject(3, pGobject);
    pstmt.setString(4, upd_text);
    pstmt.execute();
    pstmt.close();
  }

  /**
   * Given an sql connection check to see if specified system exists and has/has not been soft deleted
   * @param conn - Sql connection
   * @param tenant - name of tenant
   * @param name - name of system
   * @param includeDeleted -if soft deleted systems should be included
   * @return - true if system exists, else false
   * @throws SQLException -
   */
  private static boolean checkForTSystem(Connection conn, String tenant, String name, boolean includeDeleted)
          throws SQLException
  {
    boolean result = false;
    // Prepare the statement, fill in placeholders and execute
    String sql = SqlStatements.CHECK_FOR_SYSTEM_BY_NAME;
    if (includeDeleted) sql = SqlStatements.CHECK_FOR_SYSTEM_BY_NAME_ALL;
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setString(1, tenant);
    pstmt.setString(2, name);
    ResultSet rs = pstmt.executeQuery();
    if (rs != null && rs.next()) result = rs.getBoolean(1);
    return result;
  }

  /**
   * Persist job capabilities given an sql connection and a system
   */
  private static void persistJobCapabilities(Connection conn, TSystem tSystem, int systemId) throws SQLException
  {
    var jobCapabilities = tSystem.getJobCapabilities();
    if (jobCapabilities != null && !jobCapabilities.isEmpty()) {
      String sql = SqlStatements.ADD_CAPABILITY;
      for (Capability cap : jobCapabilities) {
        String valStr = "";
        if (cap.getValue() != null ) valStr = cap.getValue();
        // Prepare the statement and execute it
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, systemId);
        pstmt.setString(2, cap.getCategory().name());
        pstmt.setString(3, cap.getName());
        pstmt.setString(4, valStr);
        pstmt.execute();
      }
    }
  }

  /**
   * Remove job capabilities given an sql connection and a system id
   */
  private static void removeJobCapabilities(Connection conn, int systemId) throws SQLException
  {
    String sql = SqlStatements.DELETE_CAPABILITES;
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setInt(1, systemId);
    pstmt.execute();
  }

  /**
   * populateTSystem
   * Instantiate and populate an object using a result set. The cursor for the
   *   ResultSet must be advanced to the desired result.
   *
   * @param rs the result set containing one or more items, cursor at desired item
   * @return the new, fully populated job object or null if there is a problem
   * @throws TapisJDBCException - on error
   */
  private static TSystem populateTSystem(ResultSet rs, List<Capability> jobCaps) throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;

    TSystem tSystem;
    try
    {
      // Populate transfer methods
      List<TransferMethod> txfrMethodsList = new ArrayList<>();
      String txfrMethodsStr = rs.getString(13);
      if (txfrMethodsStr != null && !StringUtils.isBlank(txfrMethodsStr))
      {
        // Strip off surrounding braces and convert strings to enums
        // NOTE: All values should be valid due to enforcement of type in DB and json schema validation
        String[] txfrMethodsStrArray = (txfrMethodsStr.substring(1, txfrMethodsStr.length() - 1)).split(",");
        for (String tmech : txfrMethodsStrArray)
        {
          if (!StringUtils.isBlank(tmech)) txfrMethodsList.add(TransferMethod.valueOf(tmech));
        }
      }

      // Create the TSystem
      tSystem = new TSystem(rs.getInt(1), // id
                            rs.getString(2), // tenant
                            rs.getString(3), // name
                            rs.getString(4), // description
                            SystemType.valueOf(rs.getString(5)), // system type
                            rs.getString(6), // owner
                            rs.getString(7), // host
                            rs.getBoolean(8), //enabled
                            rs.getString(9), // effectiveUserId
                            AccessMethod.valueOf(rs.getString(10)),
                            null, // accessCredential
                            rs.getString(11), // bucketName
                            rs.getString(12), // rootDir
                            txfrMethodsList,
                            rs.getInt(14), // port
                            rs.getBoolean(15), // useProxy
                            rs.getString(16), //proxyHost
                            rs.getInt(17), // proxyPort
                            rs.getBoolean(18), // jobCanExec
                            rs.getString(19), // jobLocalWorkingDir
                            rs.getString(20), // jobLocalArchiveDir
                            rs.getString(21), // jobRemoteArchiveSystem
                            rs.getString(22), // jobRemoteArchiveSystemDir
                            jobCaps,
                            TapisGsonUtils.getGson().fromJson(rs.getString(23), String[].class), // tags
                            TapisGsonUtils.getGson().fromJson(rs.getString(24), JsonObject.class), // notes
                            rs.getTimestamp(25).toInstant(), // created
                            rs.getTimestamp(26).toInstant()); // updated
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    return tSystem;
  }

  /**
   * populateCapability
   * Instantiate and populate an object using a result set. The cursor for the
   *   ResultSet must be advanced to the desired result.
   *
   * @param rs the result set containing one or more items, cursor at desired item
   * @return the new, fully populated job object or null if there is a problem
   * @throws TapisJDBCException - on error
   */
  private static Capability populateCapability(ResultSet rs) throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;

    Capability capability;
    try
    {
      // Create the Capability
      capability = new Capability(Category.valueOf(rs.getString(1)), // category
                                  rs.getString(2), // name
                                  rs.getString(3)); // value
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    return capability;
  }

  private static List<Capability> retrieveJobCaps(int systemId, Connection conn)
          throws TapisJDBCException, SQLException
  {
    List<Capability> jobCaps = new ArrayList<>();
    String sql = SqlStatements.SELECT_SYSTEM_CAPS;
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setInt(1, systemId);
    ResultSet rsCaps = pstmt.executeQuery();
    // Iterate over results
    if (rsCaps != null)
    {
      while (rsCaps.next())
      {
        Capability cap = populateCapability(rsCaps);
        if (cap != null) jobCaps.add(cap);
      }
    }
    return jobCaps;
  }
}

package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.utexas.tacc.tapis.systems.gen.jooq.Tables.*;

import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
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
//    String transferMethodsStr = LibUtils.getTransferMethodsAsString(system.getTransferMethods());
//    TransferMethod[] txfrMethods = (TransferMethod[]) system.getTransferMethods().toArray();
    // Convert transferMethods into array of strings
    String[] transferMethodsStrArray = LibUtils.getTransferMethodsAsStringArray(system.getTransferMethods());

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    String proxyHost = TSystem.DEFAULT_PROXYHOST;
    if (system.getProxyHost() != null) proxyHost = system.getProxyHost();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Check to see if system exists or has been soft deleted. If yes then throw IllegalStateException
      boolean doesExist = checkForTSystem(db, system.getTenant(), system.getName(), true);
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
//      var tagsJsonb = new PGobject();
//      tagsJsonb.setType("jsonb");
//      tagsJsonb.setValue(tagsStr);
//      var notesJsonb =  new PGobject();
//      notesJsonb.setType("jsonb");
//      notesJsonb.setValue(notesObj.toString());

      Record record = db.insertInto(SYSTEMS)
              .set(SYSTEMS.TENANT, system.getTenant())
              .set(SYSTEMS.NAME, system.getName())
              .set(SYSTEMS.DESCRIPTION, system.getDescription())
              .set(SYSTEMS.SYSTEM_TYPE, system.getSystemType())
              .set(SYSTEMS.OWNER, owner)
              .set(SYSTEMS.HOST, system.getHost())
              .set(SYSTEMS.ENABLED, system.isEnabled())
              .set(SYSTEMS.EFFECTIVE_USER_ID, effectiveUserId)
              .set(SYSTEMS.DEFAULT_ACCESS_METHOD, system.getDefaultAccessMethod())
              .set(SYSTEMS.BUCKET_NAME, system.getBucketName())
              .set(SYSTEMS.ROOT_DIR, system.getRootDir())
              .set(SYSTEMS.TRANSFER_METHODS, transferMethodsStrArray)
              .set(SYSTEMS.PORT, system.getPort())
              .set(SYSTEMS.USE_PROXY, system.isUseProxy())
              .set(SYSTEMS.PROXY_HOST, proxyHost)
              .set(SYSTEMS.PROXY_PORT, system.getProxyPort())
              .set(SYSTEMS.JOB_CAN_EXEC, system.getJobCanExec())
              .set(SYSTEMS.JOB_LOCAL_WORKING_DIR, system.getJobLocalWorkingDir())
              .set(SYSTEMS.JOB_LOCAL_ARCHIVE_DIR, system.getJobLocalArchiveDir())
              .set(SYSTEMS.JOB_REMOTE_ARCHIVE_SYSTEM, system.getJobRemoteArchiveSystem())
              .set(SYSTEMS.JOB_REMOTE_ARCHIVE_DIR, system.getJobRemoteArchiveDir())
              .set(SYSTEMS.TAGS_JSONB, TapisGsonUtils.getGson().fromJson(tagsStr, JsonElement.class))
              .set(SYSTEMS.NOTES_JSONB, notesObj)
              .returningResult(SYSTEMS.ID)
              .fetchOne();
      systemId = record.getValue(SYSTEMS.ID);

      // Persist job capabilities
      persistJobCapabilities(conn, system, systemId);

      // Persist update record
      addUpdate(db, authenticatedUser, systemId, SystemOperation.create, createJsonStr, scrubbedText);

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
//    String transferMethodsStr = LibUtils.getTransferMethodsAsString(patchedSystem.getTransferMethods());
    // Convert transferMethods into array of strings
    String[] transferMethodsStrArray = LibUtils.getTransferMethodsAsStringArray(patchedSystem.getTransferMethods());

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    String proxyHost = TSystem.DEFAULT_PROXYHOST;
    if (patchedSystem.getProxyHost() != null) proxyHost = patchedSystem.getProxyHost();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Check to see if system exists and has not been soft deleted. If no then throw IllegalStateException
      boolean doesExist = checkForTSystem(db, tenant, name, false);
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
//      var tagsJsonb = new PGobject();
//      tagsJsonb.setType("jsonb");
//      tagsJsonb.setValue(tagsStr);
//      var notesJsonb = new PGobject();
//      notesJsonb.setType("jsonb");
//      notesJsonb.setValue(notesObj.toString());

      db.update(SYSTEMS)
              .set(SYSTEMS.DESCRIPTION, patchedSystem.getDescription())
              .set(SYSTEMS.HOST, patchedSystem.getHost())
              .set(SYSTEMS.ENABLED, patchedSystem.isEnabled())
              .set(SYSTEMS.EFFECTIVE_USER_ID, effectiveUserId)
              .set(SYSTEMS.DEFAULT_ACCESS_METHOD, patchedSystem.getDefaultAccessMethod())
              .set(SYSTEMS.TRANSFER_METHODS, transferMethodsStrArray)
              .set(SYSTEMS.PORT, patchedSystem.getPort())
              .set(SYSTEMS.USE_PROXY, patchedSystem.isUseProxy())
              .set(SYSTEMS.PROXY_HOST, proxyHost)
              .set(SYSTEMS.PROXY_PORT, patchedSystem.getProxyPort())
              .set(SYSTEMS.TAGS_JSONB, TapisGsonUtils.getGson().fromJson(tagsStr, JsonElement.class))
              .set(SYSTEMS.NOTES_JSONB, notesObj).execute();

      // If jobCapabilities updated then replace them
      if (patchSystem.getJobCapabilities() != null) {
        db.deleteFrom(CAPABILITIES).where(CAPABILITIES.SYSTEM_ID.eq(systemId)).execute();
        persistJobCapabilities(conn, patchedSystem, systemId);
      }

      // Persist update record
      addUpdate(db, authenticatedUser, systemId, SystemOperation.modify, updateJsonStr, scrubbedText);

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
    return systemId;
  }

  /**
   * Update owner of a system given system Id and new owner name
   *
   */
  @Override
  public void updateSystemOwner(AuthenticatedUser authenticatedUser, int systemId, String newOwnerName) throws TapisException
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
      DSLContext db = DSL.using(conn);
      db.update(SYSTEMS).set(SYSTEMS.OWNER, newOwnerName).where(SYSTEMS.ID.eq(systemId)).execute();
      // Persist update record
      String updateJsonStr = TapisGsonUtils.getGson().toJson(newOwnerName);
      addUpdate(db, authenticatedUser, systemId, SystemOperation.changeOwner, updateJsonStr , null);
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
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
  public int softDeleteTSystem(AuthenticatedUser authenticatedUser, int systemId) throws TapisException
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
      DSLContext db = DSL.using(conn);

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.SOFT_DELETE_SYSTEM_BY_ID;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, systemId);
      rows = pstmt.executeUpdate();

      // Persist update record
      addUpdate(db, authenticatedUser, systemId, SystemOperation.softDelete, EMPTY_JSON, null);

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
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.deleteFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenant),SYSTEMS.NAME.eq(name)).execute();
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems");
    }
    finally
    {
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
      DSLContext db = DSL.using(conn);
      // Run the sql
      result = checkForTSystem(db, tenant, name, includeDeleted);
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

    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      // ------------------------- Call SQL ----------------------------
      // Use jOOQ to build query string
      DSLContext db = DSL.using(conn);
      Result<?> result = db.select(SYSTEMS.NAME).from(SYSTEMS).where(SYSTEMS.TENANT.eq(tenant)).fetch();
      // Iterate over result
      for (Record r : result) { list.add(r.get(SYSTEMS.NAME)); }
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
  public void addUpdateRecord(AuthenticatedUser authenticatedUser, int systemId, SystemOperation op, String upd_json,
                              String upd_text) throws TapisException
  {
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      addUpdate(db, authenticatedUser, systemId, op, upd_json, upd_text);

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
  private void addUpdate(DSLContext db, AuthenticatedUser authenticatedUser, int systemId,
                         SystemOperation op, String upd_json, String upd_text)
  {
    String updJsonStr = (StringUtils.isBlank(upd_json)) ? EMPTY_JSON : upd_json;
    // Persist update record
    db.insertInto(SYSTEM_UPDATES)
            .set(SYSTEM_UPDATES.SYSTEM_ID, systemId)
            .set(SYSTEM_UPDATES.USER_NAME, authenticatedUser.getName())
            .set(SYSTEM_UPDATES.OPERATION, op)
            .set(SYSTEM_UPDATES.UPD_JSONB, TapisGsonUtils.getGson().fromJson(updJsonStr, JsonElement.class))
            .set(SYSTEM_UPDATES.UPD_TEXT, upd_text)
            .execute();
  }

  /**
   * Given an sql connection check to see if specified system exists and has/has not been soft deleted
   * @param db - jooq context
   * @param tenant - name of tenant
   * @param name - name of system
   * @param includeDeleted -if soft deleted systems should be included
   * @return - true if system exists, else false
   */
  private static boolean checkForTSystem(DSLContext db, String tenant, String name, boolean includeDeleted)
  {
    if (includeDeleted) return db.fetchExists(SYSTEMS, SYSTEMS.NAME.eq(name),SYSTEMS.TENANT.eq(tenant));
    else return db.fetchExists(SYSTEMS, SYSTEMS.NAME.eq(name),SYSTEMS.TENANT.eq(tenant),SYSTEMS.DELETED.eq(false));
  }

  /**
   * Persist job capabilities given an sql connection and a system
   */
  private static void persistJobCapabilities(Connection conn, TSystem tSystem, int systemId) throws SQLException
  {
    var jobCapabilities = tSystem.getJobCapabilities();
    if (jobCapabilities == null || jobCapabilities.isEmpty()) return;

    // Use jOOQ
    DSLContext db = DSL.using(conn);

    for (Capability cap : jobCapabilities) {
      String valStr = "";
      if (cap.getValue() != null ) valStr = cap.getValue();
      db.insertInto(CAPABILITIES).set(CAPABILITIES.SYSTEM_ID, systemId)
              .set(CAPABILITIES.CATEGORY, cap.getCategory())
              .set(CAPABILITIES.NAME, cap.getName())
              .set(CAPABILITIES.VALUE, valStr)
              .execute();
    }
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

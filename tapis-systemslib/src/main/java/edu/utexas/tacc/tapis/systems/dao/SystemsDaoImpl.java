package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Capability.Category;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
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
   * @throws TapisException - on error
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public int createTSystem(AuthenticatedUser authenticatedUser, TSystem system, String scrubbedJson)
          throws TapisException, IllegalStateException {
    // Generated sequence id
    int systemId = -1;
    // ------------------------- Check Input -------------------------
    if (authenticatedUser == null) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "authenticatedUser");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (system == null || StringUtils.isBlank(system.getTenant())) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "tenantName");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (StringUtils.isBlank(system.getName())) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "systemName");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (system.getSystemType() == null) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "systemType");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (system.getDefaultAccessMethod() == null)
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createSystem", "defaultAccessMethod");
      _log.error(msg);
      throw new TapisException(msg);
    }

    var transferMethods = TSystem.DEFAULT_TRANSFER_METHODS;
    if (system.getTransferMethods() != null) transferMethods = system.getTransferMethods();
    String transferMethodsStr = LibUtils.getTransferMethodsAsString(transferMethods);

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    String proxyHost = TSystem.DEFAULT_PROXYHOST;
    if (system.getProxyHost() != null) proxyHost = system.getProxyHost();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Check to see if system exists. If yes then throw IllegalStateException
      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.CHECK_FOR_SYSTEM_BY_NAME;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, system.getTenant());
      pstmt.setString(2, system.getName());
      ResultSet rs = pstmt.executeQuery();
      // Should get one row back. If not assume system does not exist
      boolean doesExist = false;
      if (rs != null && rs.next()) doesExist = rs.getBoolean(1);
      if (doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", authenticatedUser, system.getName()));

      // Make sure owner, effectiveUserId, notes and tags are all set
      String owner = TSystem.DEFAULT_OWNER;
      if (StringUtils.isNotBlank(system.getOwner())) owner = system.getOwner();
      String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
      if (StringUtils.isNotBlank(system.getEffectiveUserId())) effectiveUserId = system.getEffectiveUserId();
      String tagsStr = TSystem.DEFAULT_TAGS_STR;
      if (system.getTags() != null) tagsStr = TapisGsonUtils.getGson().toJson(system.getTags());
      String notesStr =  TSystem.DEFAULT_NOTES_STR;
      if (system.getNotes() != null) notesStr = system.getNotes().toString();

      // Convert tags and notes to jsonb objects.
      // Tags is a list of strings and notes is a JsonObject
      var tagsJsonb = new PGobject();
      tagsJsonb.setType("jsonb");
      tagsJsonb.setValue(tagsStr);
      var notesJsonb = new PGobject();
      notesJsonb.setType("jsonb");
      notesJsonb.setValue(notesStr);

      // Prepare the statement, fill in placeholders and execute
      sql = SqlStatements.CREATE_SYSTEM;
      pstmt = conn.prepareStatement(sql);
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
      pstmt.setString(24, scrubbedJson);
      pstmt.execute();
      // The generated sequence id should come back as result
      rs = pstmt.getResultSet();
      if (rs == null || !rs.next())
      {
        String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems");
        _log.error(msg);
        throw new TapisException(msg);
      }
      systemId = rs.getInt(1);
      rs.close();
      rs = null;

      // Persist job capabilities
      var jobCapabilities = system.getJobCapabilities();
      if (jobCapabilities != null && !jobCapabilities.isEmpty()) {
        sql = SqlStatements.ADD_CAPABILITY;
        for (Capability cap : jobCapabilities) {
          String valStr = "";
          if (cap.getValue() != null ) valStr = cap.getValue();
          // Prepare the statement and execute it
          pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, system.getTenant());
          pstmt.setInt(2, systemId);
          pstmt.setString(3, cap.getCategory().name());
          pstmt.setString(4, cap.getName());
          pstmt.setString(5, valStr);
          pstmt.execute();
        }
      }

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

      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.DELETE_SYSTEM_BY_NAME_CASCADE;
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
  public boolean checkForTSystemByName(String tenant, String name) throws TapisException {
    // Initialize result.
    boolean result = false;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();


      // Prepare the statement, fill in placeholders and execute
      String sql = SqlStatements.CHECK_FOR_SYSTEM_BY_NAME;
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tenant);
      pstmt.setString(2, name);
      ResultSet rs = pstmt.executeQuery();
      if (rs != null && rs.next()) result = rs.getBoolean(1);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rs);
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
   * @param systemName - system name
   * @return System object if found, null if not found
   * @throws TapisException - on error
   */
  @Override
  public TSystem getTSystemByName(String tenant, String systemName) throws TapisException {
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
      pstmt.setString(2, systemName);
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
      List<Capability> jobCaps = retrieveJobCaps(tenant, systemId, conn);

      // Use results to populate system object
      result = populateTSystem(rsSys, jobCaps);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, pstmt, rsSys);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_SELECT_NAME_ERROR", "System", tenant, systemName, e.getMessage());
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
   * @param tenantName - tenant name
   * @return - list of TSystem objects
   * @throws TapisException - on error
   */
  @Override
  public List<TSystem> getTSystems(String tenantName) throws TapisException
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
      pstmt.setString(1, tenantName);
      ResultSet rs = pstmt.executeQuery();
      // Iterate over results
      if (rs != null)
      {
        while (rs.next())
        {
          // Retrieve job capabilities
          int systemId = rs.getInt(1);
          List<Capability> jobCaps = retrieveJobCaps(tenantName, systemId, conn);
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

  private static List<Capability> retrieveJobCaps(String tenantName, int systemId, Connection conn)
          throws TapisJDBCException, SQLException
  {
    List<Capability> jobCaps = new ArrayList<>();
    String sql = SqlStatements.SELECT_SYSTEM_CAPS;
    PreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setString(1, tenantName);
    pstmt.setInt(2, systemId);
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

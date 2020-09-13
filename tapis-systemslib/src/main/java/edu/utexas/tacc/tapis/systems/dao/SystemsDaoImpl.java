package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.search.parser.ASTBinaryExpression;
import edu.utexas.tacc.tapis.search.parser.ASTLeaf;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTUnaryExpression;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.activemq.filter.UnaryExpression;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemsRecord;
import static edu.utexas.tacc.tapis.systems.gen.jooq.Tables.*;
import static edu.utexas.tacc.tapis.systems.gen.jooq.Tables.SYSTEMS;

import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/*
 * Class to handle persistence and queries for Tapis System objects.
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
      boolean doesExist = checkForSystem(db, system.getTenant(), system.getName(), true);
      if (doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", authenticatedUser, system.getName()));

      // Make sure owner, effectiveUserId, notes and tags are all set
      String owner = TSystem.DEFAULT_OWNER;
      if (StringUtils.isNotBlank(system.getOwner())) owner = system.getOwner();
      String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
      if (StringUtils.isNotBlank(system.getEffectiveUserId())) effectiveUserId = system.getEffectiveUserId();
      String[] tagsStrArray = TSystem.DEFAULT_TAGS;
      if (system.getTags() != null) tagsStrArray = system.getTags();
      JsonObject notesObj = TSystem.DEFAULT_NOTES;
      if (system.getNotes() != null) notesObj = (JsonObject) system.getNotes();

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
              .set(SYSTEMS.TAGS, tagsStrArray)
              .set(SYSTEMS.NOTES, notesObj)
              .returningResult(SYSTEMS.ID)
              .fetchOne();
      systemId = record.getValue(SYSTEMS.ID);

      // Persist job capabilities
      persistJobCapabilities(db, system, systemId);

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
   * Following columns will be updated:
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
      boolean doesExist = checkForSystem(db, tenant, name, false);
      if (!doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, name));

      // Make sure effectiveUserId, notes and tags are all set
      String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
      if (StringUtils.isNotBlank(patchedSystem.getEffectiveUserId())) effectiveUserId = patchedSystem.getEffectiveUserId();
      String[] tagsStrArray = TSystem.DEFAULT_TAGS;
      if (patchedSystem.getTags() != null) tagsStrArray = patchedSystem.getTags();
      JsonObject notesObj =  TSystem.DEFAULT_NOTES;
      if (patchedSystem.getNotes() != null) notesObj = (JsonObject) patchedSystem.getNotes();

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
              .set(SYSTEMS.TAGS, tagsStrArray)
              .set(SYSTEMS.NOTES, notesObj)
              .where(SYSTEMS.ID.eq(systemId))
              .execute();

      // If jobCapabilities updated then replace them
      if (patchSystem.getJobCapabilities() != null) {
        db.deleteFrom(CAPABILITIES).where(CAPABILITIES.SYSTEM_ID.eq(systemId)).execute();
        persistJobCapabilities(db, patchedSystem, systemId);
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
      // If system does not exist or has been soft deleted return 0
      if (!db.fetchExists(SYSTEMS, SYSTEMS.ID.eq(systemId), SYSTEMS.DELETED.eq(false)))
      {
        return 0;
      }
      rows = db.update(SYSTEMS).set(SYSTEMS.DELETED, true).where(SYSTEMS.ID.eq(systemId)).execute();
      // Persist update record
      addUpdate(db, authenticatedUser, systemId, SystemOperation.softDelete, EMPTY_JSON, null);

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
   * checkDB
   * Check that we can connect with DB and that the main table of the service exists.
   * @return null if all OK else return an exception
   */
  @Override
  public Exception checkDB()
  {
    Exception result = null;
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // execute SELECT to_regclass('tapis_sys.systems');
      // Build and execute a simple postgresql statement to check for the table
      String sql = "SELECT to_regclass('" + SYSTEMS.getName() + "')";
      Result<Record> ret = db.resultQuery(sql).fetch();
      if (ret == null || ret.isEmpty() || ret.getValue(0,0) == null)
      {
        result = new TapisException(LibUtils.getMsg("SYSLIB_CHECKDB_NO_TABLE", SYSTEMS.getName()));
      }
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      result = e;
      // Rollback always logs msg and throws exception.
      // In this case of a simple check we ignore the exception, we just want the log msg
      try { LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems"); }
      catch (Exception e1) { }
    }
    finally
    {
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * migrateDB
   * Use Flyway to make sure DB schema is at the latest version
   */
  @Override
  public void migrateDB() throws TapisException
  {
    Flyway flyway = Flyway.configure().dataSource(getDataSource()).load();
    flyway.migrate();
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
      result = checkForSystem(db, tenant, name, includeDeleted);
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
      DSLContext db = DSL.using(conn);
      SystemsRecord r = db.selectFrom(SYSTEMS)
              .where(SYSTEMS.TENANT.eq(tenant),SYSTEMS.NAME.eq(name),SYSTEMS.DELETED.eq(false))
              .fetchOne();
      if (r == null) return null;
      else result = r.into(TSystem.class);

      // Retrieve and set job capabilities
      result.setJobCapabilities(retrieveJobCaps(db, result.getId()));

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
   * getSystems
   * Conditions in searchList must be processed by SearchUtils.validateAndExtractSearchCondition(cond)
   *   prior to this call for proper validation and treatment of special characters.
   * @param tenant - tenant name
   * @param searchList - optional list of conditions used for searching
   * @param IDs - list of system IDs to consider. null indicates no restriction.
   * @return - list of TSystem objects
   * @throws TapisException - on error
   */
  @Override
  public List<TSystem> getTSystems(String tenant, List<String> searchList, List<Integer> IDs) throws TapisException
  {
    // The result list should always be non-null.
    var retList = new ArrayList<TSystem>();

    // If no IDs in list then we are done.
    if (IDs != null && IDs.isEmpty()) return retList;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Begin where condition for this query
      Condition whereCondition = (SYSTEMS.TENANT.eq(tenant)).and(SYSTEMS.DELETED.eq(false));

//      // DEBUG
//      // Iterate over all columns and show the type
//      Field<?>[] cols = SYSTEMS.fields();
//      for (Field<?> col : cols)
//      {
//        var dataType = col.getDataType();
//        int sqlType = dataType.getSQLType();
//        String sqlTypeName = dataType.getTypeName();
//        _log.error("Column name: " + col.getName() + " type: " + sqlTypeName);
//      }
//      // DEBUG

      // Add searchList to where condition
      whereCondition = addSearchListToWhere(whereCondition, searchList);

      // Add IN condition for list of IDs
      if (IDs != null && !IDs.isEmpty()) whereCondition = whereCondition.and(SYSTEMS.ID.in(IDs));

      // Execute the select
      Result<SystemsRecord> results = db.selectFrom(SYSTEMS).where(whereCondition).fetch();
      if (results == null || results.isEmpty()) return retList;

      // Fill in job capabilities list from aux table
      for (SystemsRecord r : results)
      {
        TSystem s = r.into(TSystem.class);
        s.setJobCapabilities(retrieveJobCaps(db, s.getId()));
        retList.add(s);
      }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
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
    return retList;
  }

  /**
   * getSystemsUsingSearchAST
   * TODO This method and getSystems almost identical. Might be a way to combine. Construct AST from searchStr?
   * Search for systems using an abstract syntax tree (AST).
   * @param tenant - tenant name
   * @param searchAST - AST containing search conditions
   * @param IDs - list of system IDs to consider. null indicates no restriction.
   * @return - list of TSystem objects
   * @throws TapisException - on error
   */
  @Override
  public List<TSystem> getTSystemsUsingSearchAST(String tenant, ASTNode searchAST, List<Integer> IDs) throws TapisException
  {
    // If searchAST null or empty delegate to getTSystems
    if (searchAST == null) return getTSystems(tenant, null, IDs);
    // The result list should always be non-null.
    var retList = new ArrayList<TSystem>();

    // If no IDs in list then we are done.
    if (IDs != null && IDs.isEmpty()) return retList;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Begin where condition for this query
      Condition whereCondition = (SYSTEMS.TENANT.eq(tenant)).and(SYSTEMS.DELETED.eq(false));

      // Add searchAST to where condition
      Condition astCondition = createConditionFromAst(searchAST);
      if (astCondition != null) whereCondition = whereCondition.and(astCondition);

      // Add IN condition for list of IDs
      if (IDs != null && !IDs.isEmpty()) whereCondition = whereCondition.and(SYSTEMS.ID.in(IDs));

      // Execute the select
      Result<SystemsRecord> results = db.selectFrom(SYSTEMS).where(whereCondition).fetch();
      if (results == null || results.isEmpty()) return retList;

      // Fill in job capabilities list from aux table
      for (SystemsRecord r : results)
      {
        TSystem s = r.into(TSystem.class);
        s.setJobCapabilities(retrieveJobCaps(db, s.getId()));
        retList.add(s);
      }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
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
    return retList;
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
      DSLContext db = DSL.using(conn);
      owner = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenant), SYSTEMS.NAME.eq(name)).fetchOne(SYSTEMS.OWNER);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
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
      DSLContext db = DSL.using(conn);
      effectiveUserId = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenant), SYSTEMS.NAME.eq(name)).fetchOne(SYSTEMS.EFFECTIVE_USER_ID);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
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
      DSLContext db = DSL.using(conn);
      systemId = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenant), SYSTEMS.NAME.eq(name)).fetchOne(SYSTEMS.ID);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
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
            .set(SYSTEM_UPDATES.USER_NAME, authenticatedUser.getOboUser())
            .set(SYSTEM_UPDATES.USER_TENANT, authenticatedUser.getOboTenantId())
            .set(SYSTEM_UPDATES.OPERATION, op)
            .set(SYSTEM_UPDATES.UPD_JSON, TapisGsonUtils.getGson().fromJson(updJsonStr, JsonElement.class))
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
  private static boolean checkForSystem(DSLContext db, String tenant, String name, boolean includeDeleted)
  {
    if (includeDeleted) return db.fetchExists(SYSTEMS, SYSTEMS.NAME.eq(name),SYSTEMS.TENANT.eq(tenant));
    else return db.fetchExists(SYSTEMS, SYSTEMS.NAME.eq(name),SYSTEMS.TENANT.eq(tenant),SYSTEMS.DELETED.eq(false));
  }

  /**
   * Persist job capabilities given an sql connection and a system
   */
  private static void persistJobCapabilities(DSLContext db, TSystem tSystem, int systemId)
  {
    var jobCapabilities = tSystem.getJobCapabilities();
    if (jobCapabilities == null || jobCapabilities.isEmpty()) return;

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
   * Get capabilities for a system from an auxiliary table
   * @param db - DB connection
   * @param systemId - system
   * @return list of capabilities
   */
  private static List<Capability> retrieveJobCaps(DSLContext db, int systemId)
  {
    List<Capability> capRecords = db.selectFrom(CAPABILITIES).where(CAPABILITIES.SYSTEM_ID.eq(systemId)).fetchInto(Capability.class);
    return capRecords;
  }

  /**
   * Add searchList to where condition. All conditions are joined using AND
   * Validate column name, search comparison operator
   *   and compatibility of column type + search operator + column value
   * @param whereCondition base where condition
   * @param searchList List of conditions to add to the base condition
   * @return resulting where condition
   * @throws TapisException on error
   */
  private static Condition addSearchListToWhere(Condition whereCondition, List<String> searchList)
          throws TapisException
  {
    if (searchList == null || searchList.isEmpty()) return whereCondition;
    // Parse searchList and add conditions to the WHERE clause
    for (String condStr : searchList)
    {
      whereCondition = addSearchCondStrToWhere(whereCondition, condStr, "AND");
    }
    return whereCondition;
  }

  /**
   * Create a condition for abstract syntax tree nodes by recursively walking the tree
   * @param astNode Abstract syntax tree node to add to the base condition
   * @return resulting condition
   * @throws TapisException on error
   */
  private static Condition createConditionFromAst(ASTNode astNode) throws TapisException
  {
    if (astNode == null || astNode instanceof ASTLeaf)
    {
      // A leaf node is a column name or value. Nothing to process since we only process a complete condition
      //   having the form column_name.op.value. We should never make it to here
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST1", (astNode == null ? "null" : astNode.toString()));
      throw new TapisException(msg);
    }
    else if (astNode instanceof ASTUnaryExpression)
    {
      // A unary node should have no operator and contain a binary node with two leaf nodes.
      // NOTE: Currently unary operators not supported. If support is provided for unary operators (such as NOT) then
      //   changes will be needed here.
      ASTUnaryExpression unaryNode = (ASTUnaryExpression) astNode;
      if (!StringUtils.isBlank(unaryNode.getOp()))
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_UNARY_OP", unaryNode.getOp(), unaryNode.toString());
        throw new TapisException(msg);
      }
      // Recursive call
      return createConditionFromAst(unaryNode.getNode());
    }
    else if (astNode instanceof ASTBinaryExpression)
    {
      // It is a binary node
      ASTBinaryExpression binaryNode = (ASTBinaryExpression) astNode;
      // Recursive call
      return createConditionFromBinaryExpression(binaryNode);
    }
    return null;
  }

  /**
   * Create a condition from an abstract syntax tree binary node
   * @param binaryNode Abstract syntax tree binary node to add to the base condition
   * @return resulting condition
   * @throws TapisException on error
   */
  private static Condition createConditionFromBinaryExpression(ASTBinaryExpression binaryNode) throws TapisException
  {
    // If we are given a null then something went very wrong.
    if (binaryNode == null)
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST2");
      throw new TapisException(msg);
    }
    // If operator is AND or OR then make recursive call for each side and join together
    // For other operators build the condition left.op.right and add it
    String op = binaryNode.getOp();
    ASTNode leftNode = binaryNode.getLeft();
    ASTNode rightNode = binaryNode.getRight();
    if (StringUtils.isBlank(op))
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST3", binaryNode.toString());
      throw new TapisException(msg);
    }
    else if (op.equalsIgnoreCase("AND"))
    {
      // Recursive calls
      Condition cond1 = createConditionFromAst(leftNode);
      Condition cond2 = createConditionFromAst(rightNode);
      if (cond1 == null || cond2 == null)
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST4", binaryNode.toString());
        throw new TapisException(msg);
      }
      return cond1.and(cond2);

    }
    else if (op.equalsIgnoreCase("OR"))
    {
      // Recursive calls
      Condition cond1 = createConditionFromAst(leftNode);
      Condition cond2 = createConditionFromAst(rightNode);
      if (cond1 == null || cond2 == null)
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST4", binaryNode.toString());
        throw new TapisException(msg);
      }
      return cond1.or(cond2);

    }
    else
    {
      // End of recursion. Create a single condition.
      // Since operator is not an AND or an OR we should have 2 unary nodes or a unary and leaf node
      String lValue;
      String rValue;
      if (leftNode instanceof ASTLeaf) lValue = ((ASTLeaf) leftNode).getValue();
      else if (leftNode instanceof ASTUnaryExpression) lValue =  ((ASTLeaf) ((ASTUnaryExpression) leftNode).getNode()).getValue();
      else
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST5", binaryNode.toString());
        throw new TapisException(msg);
      }
      if (rightNode instanceof ASTLeaf) rValue = ((ASTLeaf) rightNode).getValue();
      else if (rightNode instanceof ASTUnaryExpression) rValue =  ((ASTLeaf) ((ASTUnaryExpression) rightNode).getNode()).getValue();
      else
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST6", binaryNode.toString());
        throw new TapisException(msg);
      }
      // Build the string for the search condition, left.op.right
      String condStr = lValue + "." + binaryNode.getOp() + "." + rValue;
      // Validate and create a condition from the string
      return addSearchCondStrToWhere(null, condStr, null);
    }
  }

  /**
   * Take a string containing a single condition and create a new condition or join it to an existing condition.
   * Validate column name, search comparison operator and compatibility of column type + search operator + column value
   * @param whereCondition existing condition. If null a new condition is returned.
   * @param searchStr Single search condition in the form column_name.op.value
   * @param joinOp If whereCondition is not null use AND or OR to join the condition with the whereCondition
   * @return resulting where condition
   * @throws TapisException on error
   */
  private static Condition addSearchCondStrToWhere(Condition whereCondition, String searchStr, String joinOp)
          throws TapisException
  {
    // If we have no search string then return what we were given
    if (StringUtils.isBlank(searchStr)) return whereCondition;
    // If we are given a condition but no indication of how to join new condition to it then return what we were given
    if (whereCondition != null && StringUtils.isBlank(joinOp)) return whereCondition;
    if (whereCondition != null && joinOp != null && !joinOp.equalsIgnoreCase("AND") && !joinOp.equalsIgnoreCase("OR"))
    {
      return whereCondition;
    }

    // Parse search value into column name, operator and value
    // Format must be column_name.op.value
    String[] parsedStrArray = searchStr.split("\\.", 3);
    // Validate column name
    String column = parsedStrArray[0];
    Field<?> col = SYSTEMS.field(DSL.name(column));
    // Check for column name passed in as camelcase
    if (col == null)
    {
      col = SYSTEMS.field(DSL.name(SearchUtils.camelCaseToSnakeCase(column)));
    }
    // If column not found then it is an error
    if (col == null)
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_NO_COLUMN", SYSTEMS.getName(), DSL.name(column));
      throw new TapisException(msg);
    }
    // Validate and convert operator string
    String opStr = parsedStrArray[1].toUpperCase();
    SearchOperator op = SearchUtils.getSearchOperator(opStr);
    if (op == null)
    {
      String msg = MsgUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_OP", opStr, SYSTEMS.getName(), DSL.name(column));
      throw new TapisException(msg);
    }

    // Check that column value is compatible for column type and search operator
    String val = parsedStrArray[2];
    checkConditionValidity(col, op, val);

     // If val is a timestamp then convert the string(s) to a form suitable for SQL
    // Use a utility method since val may be a single item or a list of items, e.g. for the BETWEEN operator
    if (col.getDataType().getSQLType() == Types.TIMESTAMP)
    {
      val = SearchUtils.convertValuesToTimestamps(op, val);
    }

    // Create the condition
    Condition newCondition = createCondition(col, op, val);
    // If specified add the condition to the WHERE clause
    if (StringUtils.isBlank(joinOp)) return newCondition;
    else if (joinOp.equalsIgnoreCase("AND")) return whereCondition.and(newCondition);
    else if (joinOp.equalsIgnoreCase("OR")) return whereCondition.or(newCondition);
    return newCondition;
  }

  /**
   * Validate condition expression based on column type, search operator and column string value.
   * Use java.sql.Types for validation.
   * @param col jOOQ column
   * @param op Operator
   * @param valStr Column value as string
   * @throws TapisException on error
   */
  private static void checkConditionValidity(Field<?> col, SearchOperator op, String valStr) throws TapisException
  {
    var dataType = col.getDataType();
    int sqlType = dataType.getSQLType();
    String sqlTypeName = dataType.getTypeName();
//    var t2 = dataType.getSQLDataType();
//    var t3 = dataType.getCastTypeName();
//    var t4 = dataType.getSQLType();
//    var t5 = dataType.getType();

    // Make sure we support the sqlType
    if (SearchUtils.ALLOWED_OPS_BY_TYPE.get(sqlType) == null)
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_UNSUPPORTED_SQLTYPE", SYSTEMS.getName(), col.getName(), op.name(), sqlTypeName);
      throw new TapisException(msg);
    }
    // Check that operation is allowed for column data type
    if (!SearchUtils.ALLOWED_OPS_BY_TYPE.get(sqlType).contains(op))
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_TYPE", SYSTEMS.getName(), col.getName(), op.name(), sqlTypeName);
      throw new TapisException(msg);
    }

    // Check that value (or values for op that takes a list) are compatible with sqlType
    if (!SearchUtils.validateTypeAndValueList(sqlType, op, valStr, sqlTypeName, SYSTEMS.getName(), col.getName()))
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_VALUE", op.name(), sqlTypeName, valStr, SYSTEMS.getName(), col.getName());
      throw new TapisException(msg);
    }
  }

  /**
   * Add condition to SQL where clause given column, operator, value info
   * @param col jOOQ column
   * @param op Operator
   * @param val Column value
   * @return Resulting where clause
   */
  private static Condition createCondition(Field col, SearchOperator op, String val)
  {
    List<String> valList = Collections.emptyList();
    if (SearchUtils.listOpSet.contains(op)) valList = SearchUtils.getValueList(val);
    switch (op) {
      case EQ:
        return col.eq(val);
      case NEQ:
        return col.ne(val);
      case LT:
        return col.lt(val);
      case LTE:
        return col.le(val);
      case GT:
        return col.gt(val);
      case GTE:
        return col.ge(val);
      case LIKE:
        return col.like(val);
      case NLIKE:
        return col.notLike(val);
      case IN:
        return col.in(valList);
      case NIN:
        return col.notIn(valList);
      case BETWEEN:
        return col.between(valList.get(0), valList.get(1));
      case NBETWEEN:
        return col.notBetween(valList.get(0), valList.get(1));
    }
    return null;
  }
}

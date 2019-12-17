package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkRolePermissionShort;
import edu.utexas.tacc.tapis.security.authz.permissions.PermissionTransformer.Transformation;
import edu.utexas.tacc.tapis.security.authz.model.SkRolePermission;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkRolePermissionDao
 extends SkAbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkRolePermissionDao.class);
  
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
  public SkRolePermissionDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getAllRolePermissions:                                                 */
  /* ---------------------------------------------------------------------- */
  public List<SkRolePermission> getAllRolePermissions() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<SkRolePermission> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_ALL_PERMISSIONS;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkRolePermission obj = populateSkRolePermission(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateSkRolePermission(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "SkRolePermissions", "all", e.getMessage());
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

  /* ---------------------------------------------------------------------- */
  /* assignPermission:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Assign a permission to the named role.
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0.  
   * 
   * @param tenant the tenant
   * @param user the creating user
   * @param roleName the named role to which the permission will be assigned
   * @param permission the permission specification to be assigned to the role
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   * @throws TapisNotFoundException unknown role name
   */
  public int assignPermission(String tenant, String user, String roleName, 
                              String permission) 
   throws TapisException, TapisNotFoundException
  {
      // Inputs are all checked in the called routines.
      SkRoleDao dao = new SkRoleDao();
      
      // The parent must exist in the tenant.
      Integer roleId = dao.getRoleId(tenant, roleName);
      if (roleId == null) {
          String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
          _log.error(msg);
          throw new TapisNotFoundException(msg, roleName);
      }
      
      // Assign the permission.
      return assignPermission(tenant, user, roleId, permission);
  }
  
  /* ---------------------------------------------------------------------- */
  /* assignPermission:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Assign a named child role to the parent role with the specified id.
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0.  
   * 
   * @param tenant the tenant
   * @param user the creating user
   * @param roleName the role to which the permission will be assigned
   * @param permission the permission specification to be assigned to the role
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   */
  public int assignPermission(String tenant, String user, int roleId, 
                              String permission) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(permission)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "permission");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (roleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "roleId");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      int rows = 0;
      try
      {
          // Get a database connection.
          conn = getConnection();

          // Set the sql command.
          String sql = SqlStatements.ROLE_ADD_PERMISSION;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, permission);
          pstmt.setString(3, user);
          pstmt.setString(4, user);
          pstmt.setString(5, tenant);
          pstmt.setInt(6, roleId);

          // Issue the call. 0 rows will be returned when a duplicate
          // key conflict occurs--this is not considered an error.
          rows = pstmt.executeUpdate();

          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          // Log the exception.
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_role_permission");
          _log.error(msg, e);
          throw TapisUtils.tapisify(e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
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
      
      return rows;
  }
  
  /* ---------------------------------------------------------------------- */
  /* removePermission:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Remove a permission from a parent role.
   * 
   * If the record doesn't exist in the database, this method becomes a no-op
   * and the number of rows returned is 0.  
   * 
   * @param tenant the tenant
   * @param roleName the role from which the permission will be removed
   * @param permission the permission specification to be removed from the role
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   * @throws TapisNotFoundException unknown role name
   */
  public int removePermission(String tenant, String roleName, 
                              String permission) 
   throws TapisException, TapisNotFoundException
  {
      // Inputs are all checked in the called routines.
      SkRoleDao dao = new SkRoleDao();
      
      // The parent must exist in the tenant.
      Integer roleId = dao.getRoleId(tenant, roleName);
      if (roleId == null) {
          String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
          _log.error(msg);
          throw new TapisNotFoundException(msg, roleName);
      }
      
      // Assign the permission.
      return removePermission(tenant, roleId, permission);
  }
  
  /* ---------------------------------------------------------------------- */
  /* removePermission:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Remove a permission from a parent role.
   * 
   * If the record doesn't exist in the database, this method becomes a no-op
   * and the number of rows returned is 0.  
   * 
   * @param tenant the tenant
   * @param roleName the role to which the permission will be assigned
   * @param permission the permission specification to be assigned to the role
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   */
  public int removePermission(String tenant, int roleId, String permission) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(permission)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "permission");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (roleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignPermission", "roleId");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      int rows = 0;
      try
      {
          // Get a database connection.
          conn = getConnection();

          // Set the sql command.
          String sql = SqlStatements.ROLE_REMOVE_PERMISSION;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setInt(2, roleId);
          pstmt.setString(3, permission);
          

          // Issue the call. 0 rows will be returned when a duplicate
          // key conflict occurs--this is not considered an error.
          rows = pstmt.executeUpdate();

          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          // Log the exception.
          String msg = MsgUtils.getMsg("DB_DELETE_FAILURE", "sk_role_permission");
          _log.error(msg, e);
          throw TapisUtils.tapisify(e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
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
      
      return rows;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getMatchingPermissions:                                                */
  /* ---------------------------------------------------------------------- */
  /** Get the short permission records for permissions in a tenant that are
   * like the permission specification parameter.  It's expected that the 
   * common case is that the permSpec will contain unescaped SQL wildcard
   * characters (%, _).  The string is passed as is to SQL, so wildcard matching
   * will occur in the database.  It is the caller's responsibility to escape
   * any "%" and "_" characters that might appear in the permSpec that should
   * not be interpreted as SQL wildcards.
   * 
   * @param tenant the tenant that defines the role/permission
   * @param permSpec the permission search specification
   * @param roleId the optional role id filter
   * @return the list of short permission records that meet the search criteria
   * @throws TapisException on error
   */
  public List<SkRolePermissionShort> getMatchingPermissions(String tenant,
                                                            String permSpec, 
                                                            int roleId)
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getMatchingPermissions", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(permSpec)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getMatchingPermissions", "permSpec");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Select Query ------------------------
      // Determine the type of query we are going to issue
      // based on whether we have a valid role id.
      boolean hasRoleId = roleId > 0;
      String sql = hasRoleId ? 
                      SqlStatements.SELECT_PERMISSION_PREFIX_WITH_ROLE :
                      SqlStatements.SELECT_PERMISSION_PREFIX;
      
      // ------------------------- Call SQL ----------------------------
      // Result list.
      var list = new ArrayList<SkRolePermissionShort>();

      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, permSpec);
          if (hasRoleId) pstmt.setInt(3, roleId);

          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkRolePermissionShort obj = populateSkRolePermissionShort(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateSkRolePermissionShort(rs);
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
          
          // Log the exception.
          String msg = MsgUtils.getMsg("DB_QUERY_ERROR", "sk_role_permission", e.getMessage());
          _log.error(msg, e);
          throw TapisUtils.tapisify(e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
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
      
      return list;
  }
  
  /* ---------------------------------------------------------------------- */
  /* updatePermissions:                                                     */
  /* ---------------------------------------------------------------------- */
  public int updatePermissions(String tenant, List<Transformation> transList)
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updatePermissions", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (transList == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updatePermissions", "transList");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Easy case.
      if (transList.isEmpty()) return 0;

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      int rows = 0;
      try
      {
          // Get a database connection.
          conn = getConnection();

          // Set the sql command.
          String sql = SqlStatements.UPDATE_PERMISSION_BY_ID;
          
          // Prepare the statement once and use multiple times.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          for (var transformation : transList) {
          
              // Prepare the statement and fill in the placeholders.
              pstmt.setString(1, transformation.newPerm);
              pstmt.setString(2, tenant);
              pstmt.setInt(3, transformation.permId);

              // Issue the call. 0 rows will be returned when a duplicate
              // key conflict occurs--this is not considered an error.
              rows += pstmt.executeUpdate();
          }
          
          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          // Log the exception.
          String msg = MsgUtils.getMsg("DB_UPDATE_FAILURE", "sk_role_permission");
          _log.error(msg, e);
          throw TapisUtils.tapisify(e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
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
      
      return rows;
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* populateSkRolePermission:                                              */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkRolePermission object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkRolePermission object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private SkRolePermission populateSkRolePermission(ResultSet rs)
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
    
    // Populate the SkRolePermission object using table definition field order,
    // which is the order specified in all calling methods.
    SkRolePermission obj = new SkRolePermission();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setRoleId(rs.getInt(3));
        obj.setPermission(rs.getString(4));
        obj.setCreated(rs.getTimestamp(5).toInstant());
        obj.setCreatedby(rs.getString(6));
        obj.setUpdated(rs.getTimestamp(7).toInstant());
        obj.setUpdatedby(rs.getString(8));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
  /* ---------------------------------------------------------------------- */
  /* populateSkRolePermissionShort:                                         */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkRolePermissionShort object with a record retrieved from  
   * the database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkRolePermission object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private SkRolePermissionShort populateSkRolePermissionShort(ResultSet rs)
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
    
    // Populate the SkRolePermissionShort object using table definition field
    // order, which is the order specified in all calling methods.
    SkRolePermissionShort obj = new SkRolePermissionShort();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setRoleId(rs.getInt(3));
        obj.setPermission(rs.getString(4));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
}

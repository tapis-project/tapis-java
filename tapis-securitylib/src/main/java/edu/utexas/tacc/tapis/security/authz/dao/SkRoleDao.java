package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkRoleDao
 extends SkAbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkRoleDao.class);
  
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
  public SkRoleDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getRoles:                                                              */
  /* ---------------------------------------------------------------------- */
  public List<SkRole> getRoles() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<SkRole> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_SKROLE;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkRole obj = populateSkRole(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateSkRole(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "SkRoles", "all", e.getMessage());
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
  /* getRoleNames:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Get all role names in a tenant in alphabetic order.
   * 
   * @param tenant the tenant id
   * @return the a non-null but possibly empty list of role names
   * @throws TapisException on error
   */
  public List<String> getRoleNames(String tenant) 
    throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getRoleNames", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      ArrayList<String> names = new ArrayList<>(); // result
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_SELECT_NAMES;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
                      
          // Issue the call for the N row result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) names.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "tenant", tenant, e.getMessage());
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
      
      return names;
  }

  /* ---------------------------------------------------------------------- */
  /* getRole:                                                               */
  /* ---------------------------------------------------------------------- */
  /** Get a role by tenant and name.
   * 
   * @param tenant the role's tenant id
   * @param name the role's name
   * @return the role if found or null
   * @throws TapisException on error
   */
  public SkRole getRole(String tenant, String name) 
    throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(name)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getRole", "name");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      SkRole role = null; // result
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_SELECT_EXTENDED_BY_NAME;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, name);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          role = populateSkRole(rs);
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkRole", name, e.getMessage());
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
      
      return role;
  }

  /* ---------------------------------------------------------------------- */
  /* getRoleId:                                                             */
  /* ---------------------------------------------------------------------- */
  /** Get a role's id by tenant and name.
   * 
   * @param tenant the role's tenant id
   * @param name the role's name
   * @return the role id if found or null
   * @throws TapisException on error
   */
  public Integer getRoleId(String tenant, String name) 
    throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(name)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getRole", "name");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      Integer id = null; // result
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_SELECT_ID_BY_NAME;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, name);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          if (rs.next()) id = rs.getInt(1);
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkRole", name, e.getMessage());
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
      
      return id;
  }

  /* ---------------------------------------------------------------------- */
  /* createRole:                                                            */
  /* ---------------------------------------------------------------------- */
  /** Create a new role.
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0. 
   * 
   * @param tenant the tenant
   * @param user the creating user
   * @param roleName role name
   * @param description role description
   * @return number of rows affected (0 or 1)
   * @throws TapisException if the role is not created for any reason
   */
  public int createRole(String tenant, String user, String roleName, String description) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(roleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "roleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(description)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "description");
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
          String sql = SqlStatements.ROLE_INSERT;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, roleName);
          pstmt.setString(3, description);
          pstmt.setString(4, user);
          pstmt.setString(5, user);

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
          
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_role");
          _log.error(msg, e);
          throw new TapisException(msg, e);
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
  /* updateRoleName:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Update the name of an existing role.  Zero is returned if no row was
   * affect; 1 is returned when a row was updated.
   *
   * @param tenant the tenant
   * @param user the creating user
   * @param newRoleName current role name
   * @param newRoleName new role name
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   */
  public int updateRoleName(String tenant, String user, String roleName, String newRoleName) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleName", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleName", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(roleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleName", "roleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(newRoleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleName", "newRoleName");
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
          String sql = SqlStatements.ROLE_UPDATE_ROLENAME;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, newRoleName);
          pstmt.setTimestamp(2, new Timestamp(Instant.now().toEpochMilli()));
          pstmt.setString(3, user);
          pstmt.setString(4, tenant);
          pstmt.setString(5, roleName);

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
          
          String msg = MsgUtils.getMsg("DB_UPDATE_FAILURE", "sk_role", roleName);
          _log.error(msg, e);
          throw new TapisException(msg, e);
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
  /* updateRoleDescription:                                                 */
  /* ---------------------------------------------------------------------- */
  /** Update the description of an existing role.  Zero is returned if no row was
   * affect; 1 is returned when a row was updated.
   *
   * @param tenant the tenant
   * @param user the creating user
   * @param roleName role name
   * @param description optional new role description
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   */
  public int updateRoleDescription(String tenant, String user, String roleName, String description) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(roleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "roleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(description)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "description");
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
          String sql = SqlStatements.ROLE_UPDATE_DESCRIPTION;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, description);
          pstmt.setTimestamp(2, new Timestamp(Instant.now().toEpochMilli()));
          pstmt.setString(3, user);
          pstmt.setString(4, tenant);
          pstmt.setString(5, roleName);

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
          
          String msg = MsgUtils.getMsg("DB_UPDATE_FAILURE", "sk_role", roleName);
          _log.error(msg, e);
          throw new TapisException(msg, e);
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
  /* deleteRole:                                                            */
  /* ---------------------------------------------------------------------- */
  /** Delete a role.  If the role doesn't exist this method has no effect.
   * 
   * @param tenant the role's tenant
   * @param roleName the role name
   * @return number of rows affected by the delete
   * @throws TapisException on error
   */
  public int deleteRole(String tenant, String roleName) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(roleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteRole", "roleName");
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
          String sql = SqlStatements.ROLE_DELETE_BY_NAME;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, roleName);

          // Issue the call.
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
          String msg = MsgUtils.getMsg("DB_DELETE_FAILURE", "sk_role");
          _log.error(msg, e);
          throw new TapisException(msg, e);
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
      
      // Return the number of rows affected.
      return rows;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getDescendantRoleNames:                                                */
  /* ---------------------------------------------------------------------- */
  /** Get all the names of the children roles for the specified parent role id.
   * 
   * Note: Without modules or reorganizing packages, users from one tenant can
   *       see role information from another when going directly to this interface.
   * 
   * @param parentId the role id whose descendants are requested
   * @return a non-null list of descendant role names
   * @throws TapisException on error
   */
  public List<String> getDescendantRoleNames(int parentId) throws TapisException
  {
      // Initialize result list.
      ArrayList<String> list = new ArrayList<>();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_GET_DESCENDANT_NAMES_FOR_PARENT_ID;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setInt(1, parentId);
                      
          // Issue the call for the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) list.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("SK_SELECT_DESCENDANT_ROLES_ERROR", parentId, e.getMessage());
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
  /* getAncestorRoleNames:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Get the id of the role in the tenant and then call the actual 
   * ancestor retrieval method.  The role must exist in the tenant.
   * 
   * @param tenant the role's tenant
   * @param roleName the role's name whose ancestors are sought
   * @return the non-null list of ancestor role names
   * @throws TapisException on error including role not found
   */
  public List<String> getAncestorRoleNames(String tenant, String roleName) 
   throws TapisException, TapisNotFoundException
  {
      // Get the role id.
      Integer roleId = getRoleId(tenant, roleName);
      
      // Make sure we found the role.
      if (roleId == null) {
          String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
          _log.error(msg);
          throw new TapisNotFoundException(msg, roleName);
      }
      
      // Find the role's ancestors.
      return getAncestorRoleNames(roleId);
  }
  
  /* ---------------------------------------------------------------------- */
  /* getAncestorRoleNames:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Get all the names of the ancestor roles for the specified child role id.
   * 
   * Note: Without modules or reorganizing packages, users from one tenant can
   *       see role information from another.
   * 
   * @param childId the role id whose ancestors are requested
   * @return a non-null list of ancester role names
   * @throws TapisException on error
   */
  public List<String> getAncestorRoleNames(int childId) throws TapisException
  {
      // Initialize result list.
      ArrayList<String> list = new ArrayList<>();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_GET_ANCESTOR_NAMES_FOR_CHILD_ID;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setInt(1, childId);
                      
          // Issue the call for the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) list.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("SK_SELECT_ANCESTOR_ROLES_ERROR", childId, e.getMessage());
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
  /* getImmediatePermissions:                                               */
  /* ---------------------------------------------------------------------- */
  /** Get only the permission values directly assigned to the specified role.
   * 
   * @param tenantId the requestor's tenant
   * @param roleId the role id whose permissions are requested
   * @return a non-null, ordered list of permissions
   * @throws TapisException on error
   */
  public List<String> getImmediatePermissions(String tenantId, int roleId) 
    throws TapisException
  {
      // Initialize result list.
      ArrayList<String> perms = new ArrayList<>();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_GET_IMMEDIATE_PERMISSIONS;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenantId);
          pstmt.setInt(2, roleId);
                      
          // Issue the call for the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) perms.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("SK_SELECT_IMMEDIATE_PERMISSIONS_ERROR", roleId, e.getMessage());
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
      
      return perms;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTransitivePermissions:                                              */
  /* ---------------------------------------------------------------------- */
  /** Get all the permission values transitively associated with the 
   * specified role id.
   * 
   * Note: Without modules or reorganizing packages, users from one tenant can
   *       see role information from another.
   * 
   * @param roleId the role id whose permissions are requested
   * @return a non-null, ordered list of permissions
   * @throws TapisException on error
   */
  public List<String> getTransitivePermissions(int roleId) throws TapisException
  {
      // Initialize result list.
      ArrayList<String> perms = new ArrayList<>();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.ROLE_GET_TRANSITIVE_PERMISSIONS;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setInt(1, roleId);
          pstmt.setInt(2, roleId);
                      
          // Issue the call for the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) perms.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("SK_SELECT_TRANSITIVE_PERMISSIONS_ERROR", roleId, e.getMessage());
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
      
      return perms;
  }
  
  /* ---------------------------------------------------------------------- */
  /* queryDB:                                                               */
  /* ---------------------------------------------------------------------- */
  /** Probe connectivity to the specified table.
   * 
   * @param tableName the tenant id
   * @return 0 or 1 depending on whether the table is empty or not
   * @throws TapisException on error
   */
  public int queryDB(String tableName) 
    throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tableName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "queryDB", "tableName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      int rows = 0;
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_1;
          sql = sql.replace(":table", tableName);
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the N row result set.
          ResultSet rs = pstmt.executeQuery();
          if (rs.next()) rows = rs.getInt(1);
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "tableName", tableName, e.getMessage());
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
      
      return rows;
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* populateSkRole:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkRole object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkRole object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private SkRole populateSkRole(ResultSet rs)
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
    
    // Populate the SkRole object using table definition field order,
    // which is the order specified in all calling methods.
    SkRole obj = new SkRole();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setName(rs.getString(3));
        obj.setDescription(rs.getString(4));
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
  
}

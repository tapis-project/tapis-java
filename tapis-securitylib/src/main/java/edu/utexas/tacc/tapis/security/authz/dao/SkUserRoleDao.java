package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkUserRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkUserRoleDao
 extends SkAbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkUserRoleDao.class);
  
  // Placeholders hardcoded in sql statements.
  private static final String SQL_NAMELIST_PLACEHOLDER  = ":namelist";
  private static final String SQL_OPERATION_PLACEHOLDER = ":op";
  
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
  public SkUserRoleDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getUserRoles:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Return the roles directly assigned to a user.  This method DOES NOT
   * return the transitive closure of roles assigned to the user.
   * 
   * @return the user's immediately assigned roles
   * @throws TapisException on error
   */
  public List<SkUserRole> getUserRoles() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<SkUserRole> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_SKUSERROLE;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkUserRole obj = populateSkUserRole(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateSkUserRole(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "SkUserRoles", "all", e.getMessage());
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
  /* getUserNames:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Get the names of all users in the tenant assigned any role.  The names
   * are returned in alphabet order.
   * 
   * @param tenant the tenant being queried
   * @return a non-null, sorted list of user names r
   * @throws TapisException on error
   */
  public List<String> getUserNames(String tenant) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUserRoles", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Initialize intermediate result.
      ArrayList<String> users = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_USER_NAMES;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
                      
          // Issue the call the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) users.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkUserRole", tenant, e.getMessage());
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
      
      return users;
  }
  
  /* ---------------------------------------------------------------------- */
  /* assignUserRole:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Assign a role with the specified id to a user. The query will filter out 
   * attempts to assign a role from one tenant to a user in another tenant (an 
   * exception will be thrown because no records are available to be inserted 
   * into the sk_role_tree table).
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0. 
   * 
   * @param assignee the user being assigned the role
   * @param assigneeTenant the tenant of the user
   * @param roleId the role to which the permission will be assigned
   * @param assigner the assigning user
   * @param assignerTenant the tenant of the assigner
   * @return number of rows affected (0 or 1)
   * @throws TapisException if the role and assignee are in different tenants
   */
  public int assignUserRole(String assignee, String assigneeTenant, int roleId, 
		                    String assigner, String assignerTenant) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(assignee)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "assignee");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(assigneeTenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "assigneeTenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(assigner)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "assigner");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(assignerTenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "assignerTenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (roleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "assignRole", "roleId", roleId);
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

          // Set the sql command.  The assigneeTenant and the role's
          // tenant must be the same.  Users, however, can only be 
          // assigned roles in their tenant, though those roles may 
          // be owned by services in another tenant.
          String sql = SqlStatements.USER_ADD_ROLE_BY_ID;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, assignee);
          pstmt.setInt(2, roleId);
          pstmt.setString(3, assigner);
          pstmt.setString(4, assignerTenant);
          pstmt.setString(5, assigner);
          pstmt.setString(6, assignerTenant);
          pstmt.setString(7, assigneeTenant);
          pstmt.setInt(8, roleId);

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
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_user_role");
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
  /* removeUserRole:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Remove a role from a user.
   * 
   * @param tenant user's tenant
   * @param user the target user
   * @param roleId the role id to remove
   * @return the number of rows affect (0 or 1)
   * @throws TapisException on error
   */
  public int removeUserRole(String tenant, String user, int roleId) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeRole", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (roleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "removeRole", "roleId", roleId);
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
          String sql = SqlStatements.USER_DELETE_ROLE_BY_ID;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, user);
          pstmt.setInt(3, roleId);

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
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_user_role");
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
  /* getUserRoleNames:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Get the names of all roles assigned to this user including those assigned
   * TRANSITIVELY.  The role names are returned in alphabetic order.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null, ordered list of all roles assigned to user
   * @throws TapisException on error
   */
  public List<String> getUserRoleNames(String tenant, String user) throws TapisException
  {
      // Get the <role id, role name> tuples directly assigned to this user.
      // Input checking done here.
      List<Pair<Integer, String>> roleRecs = getUserRoleIdsAndNames(tenant, user);
      
      // Final result list.
      ArrayList<String> roleNames = new ArrayList<>();
      
      // Maybe we are done.
      if (roleRecs.isEmpty()) return roleNames;
      
      // Now populate an ordered set with all role names, including
      // transitive roles, assigned to this user. A set is used to
      // ignore duplicates.
      TreeSet<String> roleSet = new TreeSet<String>();
      SkRoleDao dao = new SkRoleDao();
      for (Pair<Integer, String> pair : roleRecs) {
          roleSet.add(pair.getRight());  // save parent role name
          List<String> list = dao.getDescendantRoleNames(pair.getLeft());
          if (!list.isEmpty()) roleSet.addAll(list);
      }
      
      // Populate the list from the ordered set.
      roleNames.addAll(roleSet);
      return roleNames;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUserPermissions:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Get the permission values (i.e., constraint strings) assigned to this 
   * user including those assigned TRANSITIVELY.  
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null, ordered list of all permission values assigned to user
   * @throws TapisException on error
   */
  public List<String> getUserPermissions(String tenant, String user) throws TapisException
  {
      // Get the role ids of roles explicitly assigned to the user.
      // Input checking done here.
      List<Integer> roleIds = getUserRoleIds(tenant, user);

      // Final result list.
      ArrayList<String>  permSpecs = new ArrayList<>();
      
      // Maybe we are done.
      if (roleIds.isEmpty()) return permSpecs;
      
      // Now populate an ordered set with all permission values, including
      // those from transitive roles, assigned to this user. A set is used 
      // to ignore duplicates.
      TreeSet<String> roleSet = new TreeSet<String>();
      SkRoleDao dao = new SkRoleDao();
      for (int roleId : roleIds) {
          List<String> list = dao.getTransitivePermissions(roleId);
          if (!list.isEmpty()) roleSet.addAll(list);
      }
      
      // Populate the list from the ordered set.
      permSpecs.addAll(roleSet);
      return permSpecs;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUserRoleIdsAndNames:                                                */
  /* ---------------------------------------------------------------------- */
  /** Get the id and names of all roles directly assigned to this user.  The
   * result DOES NOT include roles assigned transitively.  The result is a 
   * list of tuples <role id, role name> assigned to the user.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null list of all roles ids and names assigned directly to user
   * @throws TapisException on error
   */
  public List<Pair<Integer,String>> getUserRoleIdsAndNames(String tenant, String user) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUserRoleIdsAndNames", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUserRoleIdsAndNames", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Initialize intermediate result.
      ArrayList<Pair<Integer,String>> roleRecs = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.USER_SELECT_ROLE_IDS_AND_NAMES;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, user);
                      
          // Issue the call the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) {
              Pair<Integer,String> pair = Pair.of(rs.getInt(1), rs.getString(2));
              roleRecs.add(pair);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkUserRole", user, e.getMessage());
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
      
      return roleRecs;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUserRoleIds:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Get the role ids directly assigned to this user.  The result DOES NOT
   * including roles assigned transitively.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null list of all roles assigned directly to user
   * @throws TapisException on error
   */
  public List<Integer> getUserRoleIds(String tenant, String user) throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUserRoles", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUserRoles", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Initialize intermediate result.
      ArrayList<Integer> roleIds = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.USER_SELECT_ROLE_IDS;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, user);
                      
          // Issue the call the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) roleIds.add(rs.getInt(1));
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkUserRole", user, e.getMessage());
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
      
      return roleIds;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUsersWithRole:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Get the users directly or indirectly assigned a role.  Users are indirectly
   * assigned a role when they are assigned an ancestor of the specified role. 
   * For example, this implies that if a user is assigned the parent role of 
   * the role being queried, then the user is also effectively assigned the 
   * role being queried.
   * 
   * The user names are returned in alphabetic order.
   * 
   * @param tenant the user's tenant
   * @param roleName the role being queried
   * @return a non-null, sorted list of all users assigned the role
   * @throws TapisException on error
   */
  public List<String> getUsersWithRole(String tenant, String roleName) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUsersWithRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(roleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUsersWithRole", "roleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Get Ancestors -----------------------
      // Get the ancestors of the role of which there might be 0 or more.
      List<String> roleNames = null;
      try {roleNames = getAncestorRoleNames(tenant, roleName);}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("SK_ANCESTOR_ROLE_ERROR", tenant, roleName);
              _log.error(msg, e);
              throw TapisUtils.tapisify(e);  // preserve TapisNotFoundException
          }
      
      // Always add the original role name to the list.
      roleNames.add(roleName);

      // Initialize final result.
      ArrayList<String> users = new ArrayList<>();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command and replace the list placeholder
          // with a well-constructed list of role names as they should
          // appear in an SQL IN clause.
          String sql = SqlStatements.USER_SELECT_USERS_WITH_ROLE;
          String s = roleNames.stream().collect(Collectors.joining("', '", "'", "'"));
          sql = sql.replace(SQL_NAMELIST_PLACEHOLDER, s);
          
          // Prepare the statement with the filled in placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
                      
          // Issue the call and process the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) users.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkUserRole", roleName, e.getMessage());
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
      
      return users;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUsersWithPermission:                                                */
  /* ---------------------------------------------------------------------- */
  /** Get the users assigned a permission through some role.  The role itself
   * can be directly or indirectly assigned to the user using role inheritance.
   * The user names are returned in alphabetic order.
   * 
   * The permSpec parameter is an extended Shiro-based permission specification
   * that uses colons as separators, the asterisk as a wildcard character and
   * commas to define lists.  Here are examples of permission specifications:
   * 
   *    system:mytenant:read:mysystem
   *    system:mytenant:*:mysystem
   *    system:mytenant
   *    files:mytenant:read,write:mysystems
   *
   * This method recognizes the percent sign (%) as a string wildcard used
   * only in database searching.  If a percent sign appears in the permSpec
   * it is interpreted as a zero or more character wildcard.  For example,
   * the following specification would match the first three of the above
   * example specifications but not the fourth:
   * 
   *    system:mytenant:%
   * 
   * @param tenant the user's tenant
   * @param permSpec the permission specification being queried with the
   *                 optional use of percent signs for wildcard searches
   * @return a non-null, sorted list of all users assigned the role
   * @throws TapisException on error
   */
  public List<String> getUsersWithPermission(String tenant, String permSpec) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUsersWithPermission", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(permSpec)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getUsersWithPermission", "permSpec");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Prohibit permission specifications that would cause a full table scan.
      if (permSpec.startsWith("%")) {
          String msg = MsgUtils.getMsg("SK_PERM_UNRESTRICTED_SEARCH", tenant, permSpec);
          throw new TapisException(msg);
      }
      
      // Initialize final result.
      ArrayList<String> users = new ArrayList<>();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command and replace the list placeholder
          // with a well-constructed list of role names as they should
          // appear in an SQL IN clause.
          String sql = SqlStatements.USER_SELECT_USERS_WITH_PERM;
          String op = permSpec.contains("%") ? "LIKE" : "=";
          sql = sql.replace(SQL_OPERATION_PLACEHOLDER, op);
          
          // Prepare the statement with the filled in placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, permSpec);
                      
          // Issue the call and process the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) users.add(rs.getString(1));
          
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkUserRole", permSpec, e.getMessage());
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
      
      return users;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* createAndAssignRole:                                                         */
  /* ---------------------------------------------------------------------------- */
  /** Create a role and assign it to a user in one atomic operation.  The role is
   * not expected to exist and the method will fail if it does.
   * 
   * @param roleName the role name to be created.
   * @param roleTenant the tenant of the new role
   * @param description the role's description
   * @param grantee the user to be assigned the role
   * @param granteeTenant the user's tenant
   * @param grantor the requestor
   * @param grantorTenant the requestor's tenant
   * @param strict true means fail if role already exists, false is idempotent
   * @return the number of changed db records
   * @throws TapisImplException on error
   */
  public int createAndAssignRole(String roleName, String roleTenant, String description,
		                         String grantee, String granteeTenant,
                                 String grantor, String grantorTenant, boolean strict)
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(roleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "roleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(roleTenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "roleTenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(description)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "description");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(grantee)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "grantee");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(granteeTenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "granteeTenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(grantor)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "grantor");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(grantorTenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createAndAssignRole", "grantorTenant");
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

          // -------------- 1. Create Role
          // Set the sql command.
          String sql;
          if (strict) sql = SqlStatements.ROLE_INSERT_STRICT;
            else sql = SqlStatements.ROLE_INSERT;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, roleTenant);
          pstmt.setString(2, roleName);
          pstmt.setString(3, description);
          pstmt.setString(4, grantor);
          pstmt.setString(5, grantorTenant);
          pstmt.setString(6, grantor);
          pstmt.setString(7, grantorTenant);
          pstmt.setString(8, grantor);
          pstmt.setString(9, grantorTenant);
          
          // Issue the call which will fail if the role already exists
          // and strict is set.
          rows = pstmt.executeUpdate();
          
          // Close the statement.
          pstmt.close();
          
          // -------------- 2. Get Role ID
          // Get the select command.
          sql = SqlStatements.ROLE_SELECT_ID_BY_NAME;
          
          // Prepare the statement and fill in the placeholders.
          pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, roleTenant);
          pstmt.setString(2, roleName);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          int id = 0;
          if (rs.next()) id = rs.getInt(1);
          
          // Close the result and statement.
          rs.close();
          pstmt.close();
          
          // Make sure we got an id.
          if (id == 0) {
              String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", roleTenant, roleName);
              _log.error(msg);
              throw new TapisException(msg);
          }
          
          // -------------- 3. Grant User Role
          // Set the sql command.
          if (strict) sql = SqlStatements.USER_ADD_ROLE_BY_ID_STRICT;
            else sql = SqlStatements.USER_ADD_ROLE_BY_ID_NOT_STRICT;

          // Prepare the statement and fill in the placeholders.
          pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, granteeTenant);
          pstmt.setString(2, grantee);
          pstmt.setInt(3, id);
          pstmt.setString(4, grantor);
          pstmt.setString(5, grantorTenant);
          pstmt.setString(6, grantor);
          pstmt.setString(7, grantorTenant);

          // Issue the call which will fail if the user already has the role
          // and strict is set.
          rows += pstmt.executeUpdate();

          // Close the statement.
          pstmt.close();

          // Commit the transaction.
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("SK_CREATE_ASSIGN_ROLE_ERROR", granteeTenant,  
                                       grantee, roleName, e.getMessage());
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
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* populateSkUserRole:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkUserRole object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkUserRole object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private SkUserRole populateSkUserRole(ResultSet rs)
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
    
    // Populate the SkUserRole object using table definition field order,
    // which is the order specified in all calling methods.
    SkUserRole obj = new SkUserRole();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setUserName(rs.getString(3));
        obj.setRoleId(rs.getInt(4));
        obj.setCreated(rs.getTimestamp(5).toInstant());
        obj.setCreatedby(rs.getString(6));
        obj.setCreatedbyTenant(rs.getString(7));
        obj.setUpdated(rs.getTimestamp(8).toInstant());
        obj.setUpdatedby(rs.getString(9));
        obj.setUpdatedbyTenant(rs.getString(10));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getAncestorRoleNames:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Return the ancestors of the named role.  The list will never contain
   * the roleName.
   * 
   * @param roleName the role name whose ancestors are sought
   * @return a non-null list of ancestor role names that optionally includes
   *         the input roleName
   * @throws TapisException on error
   */
  private List<String> getAncestorRoleNames(String tenant, String roleName) 
   throws TapisException, TapisNotFoundException
  {
      // Get all the role's ancestors.
      SkRoleDao dao = new SkRoleDao();
      List<String> list = dao.getAncestorRoleNames(tenant, roleName);
      
      // Return result list.
      return list;
  }
}

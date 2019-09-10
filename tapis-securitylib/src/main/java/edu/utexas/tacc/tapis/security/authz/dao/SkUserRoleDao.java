package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkUserRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkUserRoleDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkUserRoleDao.class);
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // The database datasource provided by clients.
  private final DataSource _ds;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  /** This class depends on the calling code to provide a datasource for
   * db connections since this code in not part of a free-standing service.
   * 
   * @param dataSource the non-null datasource 
 * @throws TapisException 
   */
  public SkUserRoleDao() throws TapisException
  {
      _ds = SkDaoUtils.getDataSource();
  }
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getUserRoles:                                                          */
  /* ---------------------------------------------------------------------- */
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
  /* assignRole:                                                            */
  /* ---------------------------------------------------------------------- */
  /** Assign a named child role to the parent role with the specified id. It
   * is expected that all information other than the childRoleName was extracted
   * from the parent role populated from the database. Otherwise, it is possible
   * to attempt assigning a child role from one tenant to a parent in another.
   * The query will filter out such attempts and an exception will be thrown
   * because no records will be inserted into the sk_role_tree table.
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0. 
   * 
   * @param tenant the tenant
   * @param assigner the creating user
   * @param roleId the role to which the permission will be assigned
   * @return number of rows affected (0 or 1)
   * @throws TapisException if a single row is not inserted
   */
  public int assignRole(String tenant, String assigner, String assignee, int roleId) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(assigner)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "assigner");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(assignee)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignRole", "assignee");
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

          // Set the sql command.
          String sql = SqlStatements.USER_ADD_ROLE_BY_ID;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, assignee);
          pstmt.setInt(2, roleId);
          pstmt.setString(3, assigner);
          pstmt.setString(4, assigner);
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
   * transitively.  The role names are returned in alphabetic order.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null, ordered list of all roles assigned to user
   * @throws TapisException on error
   */
  public List<String> getUserRoleNames(String tenant, String user) throws TapisException
  {
      // Get the <role id, role name> tuples assigned to this user.
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
  /* getUserPermissionNames:                                                */
  /* ---------------------------------------------------------------------- */
  /** Get the names of all permissions assigned to this user including those 
   * assigned transitively.  The permission names are returned in alphabetic 
   * order.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null, ordered list of all permission names assigned to user
   * @throws TapisException on error
   */
  public List<String> getUserPermissionNames(String tenant, String user) throws TapisException
  {
      // Get the role ids of roles explicitly assigned to the user.
      // Input checking done here.
      List<Integer> roleIds = getUserRoleIds(tenant, user);

      // Final result list.
      ArrayList<String>  permNames = new ArrayList<>();
      
      // Maybe we are done.
      if (roleIds.isEmpty()) return permNames;
      
      // Now populate an ordered set with all permission names, including
      // those from transitive roles, assigned to this user. A set is used 
      // to ignore duplicates.
      TreeSet<String> roleSet = new TreeSet<String>();
      SkRoleDao dao = new SkRoleDao();
      for (int roleId : roleIds) {
          List<String> list = dao.getTransitivePermissionNames(roleId);
          if (!list.isEmpty()) roleSet.addAll(list);
      }
      
      // Populate the list from the ordered set.
      permNames.addAll(roleSet);
      return permNames;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUserPermissions:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Get the permission values (i.e., constraint strings) assigned to this 
   * user including those assigned transitively.  The permission names are 
   * returned in alphabetic order.
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
      ArrayList<String>  permNames = new ArrayList<>();
      
      // Maybe we are done.
      if (roleIds.isEmpty()) return permNames;
      
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
      permNames.addAll(roleSet);
      return permNames;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getUserRoleIdsAndNames:                                                */
  /* ---------------------------------------------------------------------- */
  /** Get the id and names of all roles assigned to this user including those 
   * assigned transitively.  The result is a list of tuples <role id, role name>
   * assigned to the user.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null list of all roles ids and names assigned to user
   * @throws TapisException on error
   */
  public List<Pair<Integer,String>> getUserRoleIdsAndNames(String tenant, String user) 
   throws TapisException
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
  /** Get the role ids assigned to this user including those assigned
   * transitively.
   * 
   * @param tenant the user's tenant
   * @param user the user name
   * @return a non-null list of all roles assigned to user
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
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getConnection:                                                         */
  /* ---------------------------------------------------------------------- */
  private Connection getConnection()
    throws TapisException
  {
    // Get the connection.
    Connection conn = null;
    try {conn = _ds.getConnection();}
      catch (Exception e) {
        String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
        _log.error(msg, e);
        throw new TapisDBConnectionException(msg, e);
      }
    
    return conn;
  }

  /* ---------------------------------------------------------------------- */
  /* populateSkUserRole:                                                  */
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

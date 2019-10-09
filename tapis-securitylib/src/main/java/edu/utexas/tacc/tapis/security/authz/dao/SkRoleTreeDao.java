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
import edu.utexas.tacc.tapis.security.authz.model.SkRoleTree;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** DAO that manages the parent/child relationships between roles.
 * 
 * @author rcardone
 */
public final class SkRoleTreeDao
 extends SkAbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkRoleTreeDao.class);
  
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
  public SkRoleTreeDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getRoleTree:                                                           */
  /* ---------------------------------------------------------------------- */
  /** Dump the sk_role_tree table.
   * 
   * @return the list of all parent/child relations
   * @throws TapisException on error
   */
  public List<SkRoleTree> getRoleTree() 
    throws TapisException
  {
      // Initialize result.
      ArrayList<SkRoleTree> list = new ArrayList<>();

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_SKROLETREE;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkRoleTree obj = populateSkRoleTree(rs);
          while (obj != null) {
            list.add(obj);
            obj = populateSkRoleTree(rs);
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
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "SkRoleTree", "all", e.getMessage());
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
  /* assignChildRole:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Assign a named child role to a parent role. This method guarantees that
   * the parent and child come from the same tenant.  This method aborts if a 
   * cycle in the role heirarchy would be created if the assignment is made.
   * 
   * If the record already exists in the database, this method becomes a no-op
   * and the number of rows returned is 0. 
   * 
   * @param tenant the tenant
   * @param user the creating user
   * @param parentRoleName the role name to which the child role will be assigned
   * @param childRoleName the name of the role to be assigned to the parent
   * @return number of rows affected (0 or 1)
   * @throws TapisException if a single row is not inserted
   * @throws TapisNotFoundException unknown parent or child role name
   */
  public int assignChildRole(String tenant, String user, String parentRoleName, 
                             String childRoleName) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.  The caller has already check the two role names.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignChildRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(user)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignChildRole", "user");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(parentRoleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignChildRole", "parentRoleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(childRoleName)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "assignChildRole", "childRoleName");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      // There are 4 database calls inside the try block.  They all use  
      // the same connection and take place in a single transaction.
      Connection conn = null;
      int rows = 0;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the ids for each of the roles.
          int parentRoleId = getRoleId(conn, tenant, user, parentRoleName);
          int childRoleId  = getRoleId(conn, tenant, user, childRoleName);
          
          // Make sure adding this parent/child relationship will not cause a cycle.
          detectCycle(conn, tenant, user, parentRoleName, parentRoleId, childRoleName);
          
          // Set the sql command.
          String sql = SqlStatements.ROLE_ADD_CHILD_ROLE_BY_ID;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setInt(2, parentRoleId);
          pstmt.setInt(3, childRoleId);
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
          
          // Log the exception.
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_role_tree");
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
  /* removeChildRole:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Remove the child role from the parent role.  Both roles must exist, but
   * if the child is not actually a child of the parent, this method becomes a 
   * no-op and the number of rows returned is 0.
   * 
   * This method retrieves the parent and child role ids in separate transactions,
   * and then calls the removal method in another transaction.  In practice,
   * this should not be a problem.
   * 
   * @param tenant the tenant
   * @param parentRoleName the role name from which the child role will be deleted
   * @param childRoleName the name of the role to be deleted from the parent
   * @return number of rows affected (0 or 1)
   * @throws TapisException on error
   * @throws TapisNotFoundException unknown parent or child role name
   */
  public int removeChildRole(String tenant, String parentRoleName, String childRoleName) 
   throws TapisException
  {
      // Inputs are all checked in the called routines.
      SkRoleDao dao = new SkRoleDao();
      
      // The parent must exist in the tenant.
      Integer parentRoleId = dao.getRoleId(tenant, parentRoleName);
      if (parentRoleId == null) {
          String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, parentRoleName);
          _log.error(msg);
          throw new TapisNotFoundException(msg, parentRoleName);
      }
      
      // The child must exist in the tenant.
      Integer childRoleId = dao.getRoleId(tenant, childRoleName);
      if (childRoleId == null) {
          String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, childRoleName);
          _log.error(msg);
          throw new TapisNotFoundException(msg, childRoleName);
      }
      
      // Assign the child.
      return removeChildRole(tenant, parentRoleId, childRoleId);
  }
  
  /* ---------------------------------------------------------------------- */
  /* removeChildRole:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Remove the child role from the parent role.  Both roles must exist, but
   * if the child is not actually a child of the parent, this method becomes a 
   * no-op and the number of rows returned is 0.
   * 
   * Note that as long as there's no way for records with parent and child 
   * from different to be inserted into the table, this method will have no
   * effect if the parent and child id used here are from different tenants.
   * 
   * @param tenant the tenant
   * @param parentRoleId the role id from which the child role id will be deleted
   * @param childRoleId the role id to be deleted from the parent role id
   * @return number of rows affected (0 or 1)
   * @throws TapisException if a single row is not inserted
   */
  public int removeChildRole(String tenant, int parentRoleId, int childRoleId) 
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeChildRole", "tenant");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (parentRoleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "removeChildRole", "parentRoleId", parentRoleId);
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (childRoleId <= 0) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "removeChildRole", "childRoleId", childRoleId);
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
          String sql = SqlStatements.ROLE_REMOVE_CHILD_ROLE_BY_ID;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setInt(2, parentRoleId);
          pstmt.setInt(3, childRoleId);

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
          String msg = MsgUtils.getMsg("DB_DELETE_FAILURE", "sk_role_tree");
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
  /* populateSkRoleTree:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkRoleTree object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkRoleTree object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws AloeJDBCException on SQL access or conversion errors
   */
  private SkRoleTree populateSkRoleTree(ResultSet rs)
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
    
    // Populate the SkRoleTree object using table definition field order,
    // which is the order specified in all calling methods.
    SkRoleTree obj = new SkRoleTree();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setParentRoleId(rs.getInt(3));
        obj.setChildRoleId(rs.getInt(4));
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
  /* detectCycle:                                                           */
  /* ---------------------------------------------------------------------- */
  /** This method throws an exception if it detects that adding a child role
   * to another role would cause a cycle in the role hierarchy.  If no cycle
   * is detected, this method simply returns.
   * 
   * The caller acquires the database connection and this method runs in the
   * already established transaction.  The caller is always responsible for 
   * connection commit/rollback/close. 
   * 
   * @param conn the caller's connection
   * @param tenant the current tenant
   * @param user the user making adding the child role
   * @param parentRoleName the prospective parent role
   * @param parentRoleId the prospective parent id
   * @param childRoleName the prospective child role
   * @throws TapisException on sql error or if a cycle is detected
   */
  private void detectCycle(Connection conn, String tenant, String user, 
                           String parentRoleName, int parentRoleId, String childRoleName) 
   throws TapisException
  {
      // A database constraint would catch this, but checking here doesn't hurt.
      if (parentRoleName.equals(childRoleName))
        {
          String msg = MsgUtils.getMsg("SK_ROLE_CYCLE_DETECTED", tenant, user,
                                       parentRoleName, childRoleName);
          _log.error(msg);
          throw new TapisException(msg);
        }
      
      // Initialize the result set.
      ArrayList<String> ancestors = new ArrayList<>();
      
      // Make the recursive ancestor query. 
      try {
          // Make sure the ancestors of the parent don't already include the child.
          String sql = SqlStatements.ROLE_GET_ANCESTOR_NAMES_FOR_CHILD_ID;
      
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setInt(1, parentRoleId);
                  
          // Issue the call for the result set.
          ResultSet rs = pstmt.executeQuery();
          while (rs.next()) ancestors.add(rs.getString(1));
      
          // Close the result and statement.
          rs.close();
          pstmt.close();
      } 
      catch (Exception e) {
          String msg = MsgUtils.getMsg("SK_SELECT_ANCESTOR_ROLES_ERROR", parentRoleId, e.getMessage());
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      
      // Make sure the child is not already an ancestor of the parent.
      if (ancestors.contains(childRoleName)) {
          String msg = MsgUtils.getMsg("SK_ROLE_CYCLE_DETECTED", tenant, user,
                                       parentRoleName, childRoleName);
          _log.error(msg);
          throw new TapisException(msg);
      }
  }
  
  /* ---------------------------------------------------------------------- */
  /* getRoleId:                                                             */
  /* ---------------------------------------------------------------------- */
  /** Get the role ID for the named role.  If the role is not found in the
   * tenant, an exception is thrown.
   *
   * The caller acquires the database connection and this method runs in the
   * already established transaction.  The caller is always responsible for 
   * connection commit/rollback/close. 
   *
   * @param conn an existing connection
   * @param tenant the tenant in which the role is defined
   * @param user the request users
   * @param roleName the role name whose id is sought
   * @return the id
   * @throws TapisException if the id is not acquired for any reason
   */
  private int getRoleId(Connection conn, String tenant, String user, String roleName) 
   throws TapisException
  {
      Integer roleId = null; // result
      try
      {
          // Get the select command.
          String sql = SqlStatements.ROLE_SELECT_ID_BY_NAME;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setString(2, roleName);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          if (rs.next()) roleId = rs.getInt(1);
          
          // Close the result and statement.
          rs.close();
          pstmt.close();
      }
      catch (Exception e)
      {
          String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "SkRoleTree", roleName, e.getMessage());
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      
      // Make sure we found the role id.
      if (roleId == null) {
          String msg = MsgUtils.getMsg("SK_ROLE_GET_ERROR", tenant, user, roleName);
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      return roleId;
  }

}

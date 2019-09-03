package edu.utexas.tacc.tapis.security.dao.sql;

/** This class centralizes most if not all SQL statements used in the Tapis Security 
 * Kernel for authorization.  The statements returned are ready for preparation and, 
 * when all placeholders are properly bound, execution.
 * 
 * @author rich
 *
 */
public class SqlStatements
{
  // Permission statements.
  public static final String PERMISSION_SELECT_BY_NAME = 
      "SELECT id, name, description FROM sk_permission where tenant = ? AND name = ?";
  public static final String PERMISSION_SELECT_EXTENDED_BY_NAME = 
      "SELECT id, name, description, created, createdby, updated, updatedby FROM sk_permission where tenant = ? AND name = ?";
  public static final String PERMISSION_SELECT_BY_ID = 
      "SELECT id, name, description FROM sk_permission where tenant = ? AND id = ?";
  public static final String PERMISSION_SELECT_EXTENDED_BY_ID = 
      "SELECT id, name, description, created, createdby, updated, updatedby FROM sk_permission where tenant = ? AND id = ?";
  public static final String PERMISSION_INSERT = 
      "INSERT INTO sk_permission (tenant, name, description, createdby, updatedby) VALUES (?, ?, ?, ?, ?)";
  public static final String PERMISSION_SELECT_ID_BY_NAME =
      "SELECT id FROM sk_permission where tenant = ? AND name = ?";
  public static final String PERMISSION_DELETE_BY_ID =
      "DELETE FROM sk_permission where tenant = ? AND id = ?";
  public static final String PERMISSION_DELETE_BY_NAME =
      "DELETE FROM sk_permission where tenant = ? AND name = ?";
  public static final String PERMISSION_UPDATE = 
      "UPDATE sk_permission SET name = ?, description = ?, updated = ?, updatedby = ? where tenant = ? AND id = ?";

  // Role statements.
  public static final String ROLE_SELECT_BY_NAME = 
      "SELECT id, name, description FROM sk_role where tenant = ? AND name = ?";
  public static final String ROLE_SELECT_EXTENDED_BY_NAME = 
      "SELECT id, name, description, created, createdby, updated, updatedby FROM sk_role where tenant = ? AND name = ?";
  public static final String ROLE_SELECT_BY_ID = 
      "SELECT id, name, description FROM sk_role where tenant = ? AND id = ?";
  public static final String ROLE_SELECT_EXTENDED_BY_ID = 
      "SELECT id, name, description, created, createdby, updated, updatedby FROM sk_role where tenant = ? AND id = ?";
  public static final String ROLE_INSERT = 
      "INSERT INTO sk_role (tenant, name, description, createdby, updatedby) VALUES (?, ?, ?, ?, ?)";
  public static final String ROLE_SELECT_ID_BY_NAME =
      "SELECT id FROM sk_role where tenant = ? AND name = ?";
  public static final String ROLE_DELETE_BY_ID =
      "DELETE FROM sk_role where tenant = ? AND id = ?";
  public static final String ROLE_DELETE_BY_NAME =
      "DELETE FROM sk_role where tenant = ? AND name = ?";
  public static final String ROLE_UPDATE = 
      "UPDATE sk_role SET name = ?, description = ?, updated = ?, updatedby = ? where tenant = ? AND id = ?";
  
  // The following select statement only grabs the permission id and tenant from the 
  // sk_permission table, but returns the role id, createdby and updatedby constants passed 
  // in from the caller.
  public static final String ROLE_ADD_PERMISSION_BY_NAME =
      "INSERT INTO sk_role_permission (tenant, role_id, permission_id, createdby, updatedby) " +
      "select p.tenant, ?, p.id, ?, ? from sk_permission p where p.tenant = ? and p.name = ?";
  public static final String ROLE_REMOVE_PERMISSION_BY_NAME =
      "DELETE FROM sk_role_permission where tenant = ? and role_id = ? and " +
      "permission_id = (select p.id from sk_permission p where p.tenant = ? and p.name = ?)";

  // Permission retrieval statements.
  public static final String ROLE_GET_ASSIGNED_PERMISSIONS_BY_NAME =
      "SELECT p.name from sk_permission p, sk_role_permission rp " +
      "where p.id = rp.permission_id and rp.tenant = ? and rp.role_id = ? " +
      "order by p.name";
  
  // This recursive query retrieves all permissions effectively assigned to 
  // a role.  Specifically, the query an alphabetized set of permission names
  // assigned to the named role id and transitively to all of its descendant
  // roles.
  public static final String ROLE_GET_PERMISSIONS_BY_NAME =
      "WITH RECURSIVE children AS ( " +
      "SELECT permission_id FROM sk_role_permission WHERE role_id = ? " +
      "UNION DISTINCT " +
      "SELECT a.child_role_id FROM sk_role_tree a, children b " +
        "WHERE a.parent_role_id = b.child_role_id " +
      ") " +
      "SELECT sk_role.name FROM children, sk_role " +
        "WHERE sk_role.id = children.child_role_id " +
        "ORDER BY sk_role.name"; 
  
  // The following select statement only grabs the permission id and tenant from the 
  // sk_role table, but returns the role id, createdby and updatedby constants passed 
  // in from the caller.
  public static final String ROLE_ADD_CHILD_ROLE_BY_NAME =
      "INSERT INTO sk_role_tree (tenant, parent_role_id, child_role_id, createdby, updatedby) " +
      "select r.tenant, ?, r.id, ?, ? from sk_role r where r.tenant = ? and r.name = ?";
  public static final String ROLE_REMOVE_CHILD_ROLE_BY_NAME =
      "DELETE FROM sk_role_tree where tenant = ? and parent_role_id = ? and " +
      "child_role_id = (select r.id from sk_role r where r.tenant = ? and r.name = ?)";
  
  // Child role name retrieval in alphabetic order.
  public static final String ROLE_GET_IMMEDIATE_CHILD_ROLE_NAMES =
      "SELECT r.name from sk_role r, sk_role_tree rt " +
      "where r.tenant = ? and r.id = rt.child_role_id and rt.parent_role_id = ? " +
      "order by r.name";
  
  // Parent role name retrieval in alphabetic order.
  public static final String ROLE_GET_IMMEDIATE_PARENT_ROLE_NAMES =
      "SELECT r.name from sk_role r, sk_role_tree rt " +
      "where r.tenant = ? and r.id = rt.parent_role_id and rt.child_role_id = ? " +
      "order by r.name";
  
  // This recursive query retrieves all the roles names that are descendants
  // of the specified role (the role whose id parameter is passed in).  The
  // query returns the child names in alphabetic order.  The application  
  // guards against introducing a cycle in the graph to avoid infinite loops. 
  // Union Distinct is used to remove duplicates since as an acyclic graph, 
  // the role hierarchy allows a node to have multiple parents.
  //
  // NOTE: This postgres-specific syntax needs to be moved to
  //       a postgres file when another database is supported.
  public static final String ROLE_GET_DESCENDANT_NAMES_FOR_PARENT_ID =
      "WITH RECURSIVE children AS ( " +
      "SELECT child_role_id FROM sk_role_tree WHERE parent_role_id = ? " +
      "UNION DISTINCT " +
      "SELECT a.child_role_id FROM sk_role_tree a, children b " +
        "WHERE a.parent_role_id = b.child_role_id " +
      ") " +
      "SELECT sk_role.name FROM children, sk_role " +
        "WHERE sk_role.id = children.child_role_id " +
        "ORDER BY sk_role.name";
  
  // Given a child role id, get all its ancestor role names.
  public static final String ROLE_GET_ANCESTOR_NAMES_FOR_CHILD_ID =
      "WITH RECURSIVE children AS ( " +
      "SELECT parent_role_id FROM sk_role_tree WHERE child_role_id = ? " +
      "UNION DISTINCT " +
      "SELECT a.parent_role_id FROM sk_role_tree a, children b " +
        "WHERE a.child_role_id = b.parent_role_id " +
      ") " +
      "SELECT sk_role.name FROM children, sk_role " +
        "WHERE sk_role.id = children.parent_role_id " +
        "ORDER BY sk_role.name";
  
  // Given a role, find all permissions assigned to that role
  // and the transitive closure of all its descendants.  The 
  // WITH statement retrieves all the descendant role ids.  The
  // query that follows the WITH is a UNION.  The first part of 
  // this UNION calculates the permission names assigned to the 
  // descendant roles discovered in the preceding recursive calls.  
  // The second part of the UNION retrieves the permission names
  // assigned to the parent role.
  public static final String ROLE_GET_TRANSITIVE_PERMISSIONS =
      "WITH RECURSIVE children AS ( " +
      "SELECT child_role_id FROM sk_role_tree WHERE parent_role_id = ? " +
      "UNION DISTINCT " +
      "SELECT a.child_role_id FROM sk_role_tree a, children b " +
        "where a.parent_role_id = b.child_role_id " +
      ") " +
      "SELECT DISTINCT p1.name AS outname " +
        "FROM children, sk_role_permission rp1, sk_permission p1 " +
        "WHERE rp1.role_id = children.child_role_id " +
        "AND rp1.permission_id = p1.id " +
      "UNION DISTINCT " +
      "SELECT DISTINCT p2.name AS outname " +
        "FROM sk_role_permission rp2, sk_permission p2 " +
        "WHERE rp2.role_id = ? " +
        "AND rp2.permission_id = p2.id " +
      "ORDER BY outname";

}

package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleTreeDao;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.security.authz.model.SkRolePermissionShort;
import edu.utexas.tacc.tapis.security.authz.permissions.ExtWildcardPermission;
import edu.utexas.tacc.tapis.security.authz.permissions.PermissionTransformer;
import edu.utexas.tacc.tapis.security.authz.permissions.PermissionTransformer.Transformation;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.TapisDBUtils;

/** This singleton class implements the backend role APIs.
 * 
 * Methods in this class do not depend on the caller to validate their parameters,
 * though front-end code may do parameter validation before calling methods in
 * this class.  Many methods in this class depend on the DAO methods they call for 
 * parameter validation.
 * 
 * Methods in this class should only expose their clients to Tapis exceptions.
 * For example, SQL errors are wrapped in TapisExceptions.
 * 
 * @author rcardone
 */
public final class RoleImpl 
 extends BaseImpl
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RoleImpl.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singleton instance of this class.
    private static RoleImpl _instance;
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private RoleImpl() {}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    public static RoleImpl getInstance()
    {
        // Create the singleton instance if necessary.
        if (_instance == null) {
            synchronized (RoleImpl.class) {
                if (_instance == null) _instance = new RoleImpl();
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRoleNames:                                                          */
    /* ---------------------------------------------------------------------- */
    public List<String> getRoleNames(String tenant) throws TapisImplException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR); 
            }
        
        // Create the role.
        List<String> list = null;
        try {list = dao.getRoleNames(tenant);} 
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ROLE_GET_NAMES_ERROR", 
                                             tenant, "<unknown>");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);     
            }
        
        return list;
    }

    /* ---------------------------------------------------------------------- */
    /* getRoleByName:                                                         */
    /* ---------------------------------------------------------------------- */
    public SkRole getRoleByName(String tenant, String roleName) throws TapisImplException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR); 
            }
        
        // Create the role.
        SkRole role = null;
        try {role = dao.getRole(tenant, roleName);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ROLE_GET_ERROR", tenant, "<unknown>", 
                                             roleName);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST); 
            }

        return role;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createRole:                                                            */
    /* ---------------------------------------------------------------------- */
    public int createRole(String tenant, String user, String roleName, 
                          String description) 
     throws TapisImplException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {rows = dao.createRole(tenant, user, roleName, description);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ROLE_CREATE_ERROR", 
                                             tenant, user, roleName);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createRole:                                                            */
    /* ---------------------------------------------------------------------- */
    public int deleteRoleByName(String tenant, String roleName) throws TapisImplException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR); 
            }
        
        // Create the role.
        int rows = 0;
        try {
            rows = dao.deleteRole(tenant, roleName);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_ROLE_DELETE_ERROR", tenant, "<unknown>", 
                                         roleName);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        }

        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRolePermissions:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Get a role's permissions.  Return permissions that are directly assigned
     * to the role or transitively assigned to the role depending on the 
     * immediate flag.  The role must exist in the tenant.
     * 
     * @param tenant the role's tenant
     * @param roleName the name name
     * @param immediate true means return only directly assigned permission,
     *          false means return directly and transitively assigned permissions.
     * @return the non-null list
     * @throws TapisImplException on error
     */
    public List<String> getRolePermissions(String tenant, String roleName, 
                                           boolean immediate) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR); 
            }
        
        // Create the role.
        SkRole role = null;
        try {role = dao.getRole(tenant, roleName);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ROLE_GET_ERROR", tenant, "<unknown>", 
                                             roleName);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST); 
            }

        // We need a role.
        if (role == null) {
            String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
            _log.error(msg);
            throw new TapisNotFoundException(msg, roleName);
        }
        
        // Get requested set of permission.
        List<String> perms = null;
        if (immediate) 
            try {perms = role.getImmediatePermissions();}
            catch (Exception e) {
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);
            }
        else 
            try {perms = role.getTransitivePermissions();}
            catch (Exception e) {
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);
            }
        return perms;
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateRoleName:                                                        */
    /* ---------------------------------------------------------------------- */
    public int updateRoleName(String tenant, String user, String roleName,
                              String newRoleName) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);         
             }
        
        // Create the role.
        int rows = 0;
        try {rows = dao.updateRoleName(tenant, user, roleName, newRoleName);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", tenant, user, roleName);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);  
            }
        
        // Treat misguided updates as errors.
        if (rows == 0) {
            String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
            _log.error(msg);
            throw new TapisNotFoundException(msg, roleName);
        }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateRoleDescription:                                                 */
    /* ---------------------------------------------------------------------- */
    public int updateRoleDescription(String tenant, String user, String roleName,
                                     String description) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);         
             }
        
        // Create the role.
        int rows = 0;
        try {rows = dao.updateRoleDescription(tenant, user, roleName, description);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ROLE_UPDATE_ERROR", tenant, user, roleName);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);  
            }
        
        // Treat misguided updates as errors.
        if (rows == 0) {
            String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
            _log.error(msg);
            throw new TapisNotFoundException(msg, roleName);
        }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* addRolePermission:                                                     */
    /* ---------------------------------------------------------------------- */
    public int addRolePermission(String tenant, String user, String roleName,
                                 String permSpec) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRolePermissionDao dao = null;
        try {dao = getSkRolePermissionDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {rows = dao.assignPermission(tenant, user, roleName, permSpec);}
            catch (TapisNotFoundException e) {
                _log.error(e.getMessage());
                throw e;
            } 
            catch (Exception e) {
                // We assume a bad request for all other errors.
                String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", 
                                             tenant, user, permSpec, roleName);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST); 
            }
    
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeRolePermission:                                                  */
    /* ---------------------------------------------------------------------- */
    public int removeRolePermission(String tenant, String roleName, String permSpec) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRolePermissionDao dao = null;
        try {dao = getSkRolePermissionDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {
            rows = dao.removePermission(tenant, roleName, permSpec);
        } catch (TapisNotFoundException e) {
            _log.error(e.getMessage());
            throw e;        
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_REMOVE_PERMISSION_ERROR", 
                                         tenant, "<unknown>", permSpec, roleName);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST); 
        }

        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* addChildRole:                                                          */
    /* ---------------------------------------------------------------------- */
    public int addChildRole(String tenant, String user, String parentRoleName, 
                            String childRoleName) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRoleTreeDao dao = null;
        try {dao = getSkRoleTreeDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roleTree");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {
            rows = dao.assignChildRole(tenant, user, parentRoleName, childRoleName);
        } catch (TapisNotFoundException e) {
            _log.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_ADD_CHILD_ROLE_ERROR", 
                                         tenant, user, childRoleName, parentRoleName);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST); 
        }

        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeChildRole:                                                       */
    /* ---------------------------------------------------------------------- */
    public int removeChildRole(String tenant, String parentRoleName, String childRoleName) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkRoleTreeDao dao = null;
        try {dao = getSkRoleTreeDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roleTree");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {
            rows = dao.removeChildRole(tenant, parentRoleName, childRoleName);
        } catch (TapisNotFoundException e) {
            _log.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_DELETE_CHILD_ROLE_ERROR", 
                                         tenant, "<unknown>", childRoleName, parentRoleName);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        }

        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* previewPathPrefix:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Calculate the list of permission transformations without actually 
     * performing any changes to any permission.  The transformations are those
     * that would be applied if the replacePathPrefix() method was called.
     * 
     * @param schema the 1st part of the permission
     * @param roleName optional filter that restricts permission changes to one role
     * @param oldSystemId the value of the current system id part
     * @param newSystemId the value of the new system id part
     * @param oldPrefix the value of the current path prefix
     * @param newPrefix the value of the new path prefix
     * @param tenant the tenant id
     * @return a list of prospective transformations
     * @throws TapisImplException on error
     */
    public List<Transformation> previewPathPrefix(String schema, String roleName, 
                                                  String oldSystemId, String newSystemId, 
                                                  String oldPrefix, String newPrefix,
                                                  String tenant)
     throws TapisImplException
    {
        // Make sure the schema is one that we know uses extended path semantics.
        // The index can be no lower than 3 since the minimum schema to support
        // path semantic must start with the schema name and also include the 
        // tenant, system and path.  These 4 parts are always required.
        int pathIndex = ExtWildcardPermission.getRecursivePathIndex(schema);
        if (pathIndex < 3) {
            String msg = MsgUtils.getMsg("SK_PERM_NO_PATH_SUPPORT", schema);
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // Retrieve the role id if a role name is given.
        SkRole role = null;
        int roleId  = -1;
        if (!StringUtils.isBlank(roleName)) {
            role = getRoleByName(tenant, roleName);
            if (role == null) {
                String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
                _log.error(msg);
                throw new TapisImplException(msg, Condition.BAD_REQUEST);
            }
            roleId = role.getId();
        }
        
        // Create the permission search template.
        String permSpec = getPermissionSpec(schema, tenant, oldSystemId, 
                                            oldPrefix, pathIndex);
        
        // Get the dao.
        SkRolePermissionDao dao = null;
        try {dao = getSkRolePermissionDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Get the short records from the database.
        List<SkRolePermissionShort> dblist = null;
        try {dblist = dao.getMatchingPermissions(tenant, permSpec, roleId);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_PERM_SEARCH_ERROR", permSpec, roleId);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }
        
        // Maybe there's nothing to do.
        if (dblist.isEmpty()) return new ArrayList<Transformation>(0);
            
        // Create a transformer object.  The index parameter is for the permission
        // part that is before the path part (i.e., the system part).
        String oldText = oldSystemId + ":" + oldPrefix;
        String newText = newSystemId + ":" + newPrefix;
        var transformer = new PermissionTransformer(pathIndex-1, oldText, newText);
        
        // Calculate the transformations. Exceptions indicate a bug.
        try {transformer.addTransformations(dblist);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_PERM_TRANSFORM_ERROR", permSpec, 
                                             oldText, newText, tenant);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Return the transformations.
        return transformer.getTransformations();
    }
    
    /* ---------------------------------------------------------------------- */
    /* replacePathPrefix:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Calculate the list of permission transformations and then apply them.
     * 
     * @param schema the 1st part of the permission
     * @param roleName optional filter that restricts permission changes to one role
     * @param oldSystemId the value of the current system id part
     * @param newSystemId the value of the new system id part
     * @param oldPrefix the value of the current path prefix
     * @param newPrefix the value of the new path prefix
     * @param tenant the tenant id
     * @return a list of prospective transformations
     * @throws TapisImplException on error
     */
    public int replacePathPrefix(String schema, String roleName, 
                                 String oldSystemId, String newSystemId, 
                                 String oldPrefix, String newPrefix,
                                 String tenant)
     throws TapisImplException
    {
        // Get the list of transformation to apply.
        List<Transformation> transList = previewPathPrefix(schema, roleName, 
                                                           oldSystemId, newSystemId, 
                                                           oldPrefix, newPrefix, 
                                                           tenant);
        
        // Update the selected permissions.
        int rows = updatePermissions(tenant, transList);
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* queryDB:                                                               */
    /* ---------------------------------------------------------------------- */
    public int queryDB(String tableName) throws TapisImplException
    {
        // Get the dao.
        SkRoleDao dao = null;
        try {dao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);         
             }
        
        // Access the table.
        int rows = 0;
        try {rows = dao.queryDB(tableName);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("DB_QUERY_DB_ERROR", tableName);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);         
         }

        return rows;
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* updatePermissions:                                                     */
    /* ---------------------------------------------------------------------- */
    private int updatePermissions(String tenant, List<Transformation> transList)
     throws TapisImplException
    {
        // If there's nothing to do, let's not bother.
        if (transList.isEmpty()) return 0;
        
        // Get the dao.
        SkRolePermissionDao dao = null;
        try {dao = getSkRolePermissionDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {rows = dao.updatePermissions(tenant, transList);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_PERM_UPDATE_LIST_ERROR", 
                                         tenant, transList.size());
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST); 
        }

        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getPermissionSpec:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Construct a permission search string that inserts SQL wildcards in parts 
     * of the string that we allow any value to appear.
     * 
     * @param schema the permission schema
     * @param tenant the permission tenant
     * @param oldSystemId the search system
     * @param oldPrefix the search path
     * @param pathIndex the index in the schema of the path part
     * @return the search string
     */
    private String getPermissionSpec(String schema, String tenant, String oldSystemId, 
                                     String oldPrefix, int pathIndex)
    {
        // Create a buffer with reasonable initial capacity.
        StringBuilder buf = new StringBuilder(150);
        
        // Add in the standard permission prefix that must apply
        // to all extended path supporting permission schemas.
        // We escape the tenant in case it contains an underscore.
        buf.append(schema);
        buf.append(":");
        buf.append(TapisDBUtils.escapeSqlWildcards(tenant));
        buf.append(":");
        
        // Add any intermediate permission parts as don't care search
        // elements.  We use the SQL wildcard to indicate don't care.
        // Parts with indexes 0 and 1 are already in the buffer.  The 
        // last two parts are always the system id and path, so we only 
        // need to fill in wildcard parts if they exist between the 
        // beginning and end of the permission schema.  For example,
        // in the files schema the only intervening part defines
        // operations like read/write/execute.
        for (int i = 2; i < pathIndex - 1; i++) buf.append("%:");
    
        // Append the escaped system and path parts.
        buf.append(TapisDBUtils.escapeSqlWildcards(oldSystemId));
        buf.append(":");
        buf.append(TapisDBUtils.escapeSqlWildcards(oldPrefix));
        
        // Append the SQL wildcard.
        buf.append("%");
                
        return buf.toString();
    }
}

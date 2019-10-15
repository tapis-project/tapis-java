package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkUserRoleDao;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This singleton class implements the backend user APIs. 
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
public final class UserImpl
 extends BaseImpl
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(UserImpl.class);
    
    /* **************************************************************************** */
    /*                                     Enums                                    */
    /* **************************************************************************** */
    // Logical operations applied during authentication.
    public enum AuthOperation {ANY, ALL}
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singleton instance of this class.
    private static UserImpl _instance;
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private UserImpl() {}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                          */
    /* ---------------------------------------------------------------------- */
    public static UserImpl getInstance()
    {
        // Create the singleton instance if necessary.
        if (_instance == null) {
            synchronized (UserImpl.class) {
                if (_instance == null) _instance = new UserImpl();
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getUserNames:                                                          */
    /* ---------------------------------------------------------------------- */
    public List<String> getUserNames(String tenant) throws TapisImplException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
    
        // Get the names.
        List<String> users = null;
        try {users = dao.getUserNames(tenant);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_USER_GET_NAMES_ERROR", 
                                             tenant, e.getMessage());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
        }
    
        return users;
    }

    /* ---------------------------------------------------------------------- */
    /* getUserRoles:                                                          */
    /* ---------------------------------------------------------------------- */
    public List<String> getUserRoles(String tenant, String user) throws TapisImplException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }

        // Get the names.
        List<String> roles = null;
        try {roles = dao.getUserRoleNames(tenant, user);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                             tenant, user, e.getMessage());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
            }

        return roles;
    }

    /* ---------------------------------------------------------------------- */
    /* getUserPerms:                                                          */
    /* ---------------------------------------------------------------------- */
    public List<String> getUserPerms(String tenant, String user) throws TapisImplException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);            
            }

        // Get the names.
        List<String> perms = null;
        try {perms = dao.getUserPermissions(tenant, user);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_USER_GET_PERMISSIONS_ERROR", 
                                             tenant, user, e.getMessage());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);            
            }

        return perms;
    }

    /* ---------------------------------------------------------------------- */
    /* grantRole:                                                             */
    /* ---------------------------------------------------------------------- */
    public int grantRole(String tenant, String requestor, String user, String roleName) 
      throws TapisImplException, TapisNotFoundException
    {
        // Get the role id.
        int roleId = 0;
        try {roleId = getRoleId(tenant, roleName);}
            catch (TapisNotFoundException e) {
                _log.error(e.getMessage());
                throw e;
            }
            catch (Exception e) {
                _log.error(e.getMessage());
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);            
            }

        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }

        // Assign the role to the user.
        int rows = 0;
        try {rows = dao.assignUserRole(tenant, requestor, user, roleId);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ADD_USER_ROLE_ERROR", requestor, 
                                             tenant, roleId, user);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);            
            }
        
        return rows;
    }

    /* ---------------------------------------------------------------------- */
    /* removeRole:                                                            */
    /* ---------------------------------------------------------------------- */
    public int removeRole(String tenant, String user, String roleName) 
      throws TapisImplException, TapisNotFoundException
    {
        // Get the role id.
        int roleId = 0;
        try {roleId = getRoleId(tenant, roleName);}
            catch (TapisNotFoundException e) {
                _log.error(e.getMessage());
                throw e;
            }
            catch (Exception e) {
                _log.error(e.getMessage());
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);            
            }

        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }

        // Assign the role to the user.
        int rows = 0;
        try {rows = dao.removeUserRole(tenant, user, roleId);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_REMOVE_USER_ROLE_ERROR",  
                                             tenant, roleId, user, e.getMessage());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);            
            }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* grantRoleWithPermission:                                               */
    /* ---------------------------------------------------------------------- */
    public int grantRoleWithPermission(String tenant, String requestor, 
                                       String user, String roleName, String permSpec)
        throws TapisImplException, TapisNotFoundException
    {
        // ******************* Insert Permission into Role ********************
        // --------------------------------------------------------------------
        // Get the role id.
        int roleId = 0;
        try {roleId = getRoleId(tenant, roleName);}
            catch (TapisNotFoundException e) {
                _log.error(e.getMessage());
                throw e;
            }
            catch (Exception e) {
                _log.error(e.getMessage());
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);           
            }

        // Get the dao.
        SkRolePermissionDao rolePermDao = null;
        try {rolePermDao = getSkRolePermissionDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "rolePermission");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {
            rows = rolePermDao.assignPermission(tenant, requestor, roleId, permSpec);
        } catch (TapisNotFoundException e) {
            // This only occurs when the role name is not found.
            String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", 
                                         tenant, requestor, permSpec, roleName);
            _log.error(msg, e);
            throw e;
        } catch (Exception e) {
            // We assume a bad request for all other errors.
            String msg = MsgUtils.getMsg("SK_ADD_PERMISSION_ERROR", 
                                         tenant, requestor, permSpec, roleName);
            _log.error(msg, e);
            throw new TapisImplException(msg, e, Condition.BAD_REQUEST);        
        }
        
        // ************************ Assign Role to User ***********************
        // --------------------------------------------------------------------
        // Get the dao.
        SkUserRoleDao userDao = null;
        try {userDao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);            }

        // Assign the role to the user.
        try {rows += userDao.assignUserRole(tenant, requestor, user, roleId);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ADD_USER_ROLE_ERROR", requestor, 
                                             tenant, roleId, user);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);
            }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getUserRoleNames:                                                      */
    /* ---------------------------------------------------------------------- */
    public List<String> getUserRoleNames(String tenant, String user) 
     throws TapisImplException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);             }

        // Get the user's role names including those assigned transitively.
        List<String> roles = null;
        try {roles = dao.getUserRoleNames(tenant, user);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                             tenant, user, e.getMessage());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);            }
        
        return roles;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getUsersWithRole:                                                      */
    /* ---------------------------------------------------------------------- */
    public List<String> getUsersWithRole(String tenant, String roleName) 
     throws TapisImplException, TapisNotFoundException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR); 
            }

        // Assign the role to the user.
        List<String> users = null;
        try {users = dao.getUsersWithRole(tenant, roleName);}
            catch (TapisNotFoundException e) {
                _log.error(e.getMessage());
                throw e;
            }
            catch (Exception e) {
                _log.error(e.getMessage());
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);             }
        
        return users;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getUsersWithPermission:                                                */
    /* ---------------------------------------------------------------------- */
    public List<String> getUsersWithPermission(String tenant, String permSpec) 
     throws TapisImplException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);
            }

        // Assign the role to the user.
        List<String> users = null;
        try {users = dao.getUsersWithPermission(tenant, permSpec);}
            catch (Exception e) {
                _log.error(e.getMessage());
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        return users;
    }
    
    /* ---------------------------------------------------------------------- */
    /* hasRole:                                                               */
    /* ---------------------------------------------------------------------- */
    public boolean hasRole(String tenant, String user, String[] roleNames, AuthOperation op) 
     throws TapisImplException
    {
        // Check inputs not checked by called routines.
        if (op == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "op");            
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);            
        }
        if (roleNames == null || (roleNames.length == 0)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "roleNames");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);  
        }
        
        // Get the user's roles.  An exception can be thrown here.
        List<String> roles = getUserRoleNames(tenant, user);
        
        // Initialize the result based on the operation.
        // ANY starts out as false, ALL starts as true.
        boolean authorized = (op == AuthOperation.ANY) ? false : true;
        
        // Iterate through the list of user-suppled role names.
        for (String curRole : roleNames) {
            // Search for the role in the list whose elements are sorted in ascending order.
            int position = Collections.binarySearch(roles, curRole);
            
            // We stop processing ANY constraints as soon as we find the first match.
            if (op == AuthOperation.ANY) {
                if (position >= 0) {
                    authorized = true;
                    break;
                }
            }
            // We stop processing ALL constraints as soon as we find the first non-match.
            else {
                if (position < 0) {
                    authorized = false;
                    break;
                }
            }
        }
        
        return authorized;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getUserPermissions:                                                    */
    /* ---------------------------------------------------------------------- */
    public List<String> getUserPermissions(String tenant, String user) 
     throws TapisImplException
    {
        // Get the dao.
        SkUserRoleDao dao = null;
        try {dao = getSkUserRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "userRoles");
                _log.error(msg, e);
                throw new TapisImplException(e.getMessage(), e, Condition.INTERNAL_SERVER_ERROR); 
            }

        // Get the names.
        List<String> assignedPerms = null;
        try {assignedPerms = dao.getUserPermissions(tenant, user);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                             tenant, user, e.getMessage());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);            
            }
        
        return assignedPerms;
    }
    
    /* ---------------------------------------------------------------------- */
    /* isPermitted:                                                           */
    /* ---------------------------------------------------------------------- */
    public boolean isPermitted(String tenant, String user, String[] permSpecs, 
                               AuthOperation op) 
     throws TapisImplException
    {
        // Check inputs not checked by called routines.
        if (op == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermitted", "op");            
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);            
        }
        if (permSpecs == null || (permSpecs.length == 0)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermitted", "permSpecs");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);  
        }
        
        // Get all permissions assigned to user include those assigned transitively.
        // This call can throw an exception.
        List<String> assignedPerms = getUserPermissions(tenant, user);
        
        // Maybe it's already obvious that the user does not have permission.
        if (assignedPerms.isEmpty()) return false;
        
        // Initialize the result based on the operation.
        // ANY starts out as false, ALL starts as true.
        boolean authorized = (op == AuthOperation.ANY) ? false : true;
        
        // Create a permission cache that allows us to allocate at most
        // one wildcard object for each assigned permission.  The cache
        // is only useful if more than 1 permSpec might get tested.
        HashMap<String,WildcardPermission> assignedPermMap;
        if (permSpecs.length > 1) 
            assignedPermMap = new HashMap<>(1 + 2 * assignedPerms.size());
          else assignedPermMap = null;
        
        // Iterate through the list of user-suppled role names.
        for (String curPermSpec : permSpecs) 
        {
            // Match the current user-supplied permission with those assigned to the user.
            boolean matched = matchPermission(curPermSpec, assignedPerms, assignedPermMap);
            
            // We stop processing ANY constraints as soon as we find the first match.
            if (op == AuthOperation.ANY) {
                if (matched) {
                    authorized = true;
                    break;
                }
            }
            // We stop processing ALL constraints as soon as we find the first non-match.
            else {
                if (!matched) {
                    authorized = false;
                    break;
                }
            }
        }
        
        return authorized;
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* matchPermission:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Perform the extended Shiro-base permission checking.  All permission checking
     * is case-sensitive.  
     * 
     * The caller may provide a map to use as a cache for permission objects.  A cache 
     * will reduce the number of object created when this method is going to be called
     * with different reqPermStr's in a row, all using the same set of assignedPermStrs.
     * 
     * @param reqPermStr the spec to be matched on a user request
     * @param assignedPermStrs the user's assigned permissions
     * @param assignedPermMap the optional cache of permission strings to objects
     * @return true if permSpec matches one of the perms, false otherwise
     */
    private boolean matchPermission(String reqPermStr, List<String> assignedPermStrs,
                                    HashMap<String,WildcardPermission> assignedPermMap)
    {
        // Create a case-sensitive request permission.
        WildcardPermission reqPerm = new WildcardPermission(reqPermStr, true);
        
        // See if any of the user's assigned permissions match the request spec.
        for (String curAssignedPermStr : assignedPermStrs) 
        {
            // Declare the current perm object.
            WildcardPermission curAssignedPerm;
            
            // If caching is activated, determine if we've already created a 
            // perm object for this assigned perm string.
            if (assignedPermMap != null) {
                curAssignedPerm = assignedPermMap.get(curAssignedPermStr);
                if (curAssignedPerm == null) {
                    // Create and cache the perm object.
                    curAssignedPerm = new WildcardPermission(curAssignedPermStr, true);
                    assignedPermMap.put(curAssignedPermStr, curAssignedPerm);
                }
            }
            else curAssignedPerm = new WildcardPermission(curAssignedPermStr, true);
            
            // Check the request permission and return as soon 
            // as we find a match.
            if (curAssignedPerm.implies(reqPerm)) return true;
        }
        
        // No match if we get here.
        return false;
    }
}

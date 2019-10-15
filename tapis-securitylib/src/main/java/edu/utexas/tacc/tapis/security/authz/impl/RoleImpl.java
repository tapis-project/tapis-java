package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleTreeDao;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

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
    /* getInstance:                                                          */
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
}

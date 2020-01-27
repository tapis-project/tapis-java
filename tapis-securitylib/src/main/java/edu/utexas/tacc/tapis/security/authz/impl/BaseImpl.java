package edu.utexas.tacc.tapis.security.authz.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleTreeDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkUserRoleDao;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

abstract class BaseImpl 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(BaseImpl.class);
    
    // We reserve this character for SK generated names.  In particular, SK generated
    // role names always begin this character and users cannot create such names.
    //
    // To make our life easier, tapis reserved characters that might appear in a
    // URL should be URL safe.  These include alphanumerics [0-9a-zA-Z], 
    // special characters $-_.+!*'(), and URL reserved characters ; / ? : @ = &.
    // In particular, these characters are not URL-safe and need to be escaped: 
    // " < > # % { } | \ ^ ~ [ ] ` and space.
    public static final char RESERVED_NAME_CHAR = '$';
    
    // SK generated role names start with a reserved 2 character sequence.
    public static final String USER_DEFAULT_ROLE_PREFIX = RESERVED_NAME_CHAR + "$";

    // Role name max characters allowed in database.
    public static final int MAX_USER_NAME_LEN = 58;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // We share all dao's among all instances of this class.
    private static SkRoleDao           _roleDao;
    private static SkRoleTreeDao       _roleTreeDao;
    private static SkRolePermissionDao _rolePermissionDao;
    private static SkUserRoleDao       _userRoleDao;
    
    /* **************************************************************************** */
    /*                             Protected Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getSkRoleDao:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** Create the shared dao on first reference.
     * 
     * @return the dao
     * @throws TapisException on error
     */
    protected static SkRoleDao getSkRoleDao() 
     throws TapisException
    {
        // Avoid synchronizing exception for initialization.
        if (_roleDao == null) 
            synchronized (BaseImpl.class) {
                if (_roleDao == null) _roleDao = new SkRoleDao();
           }
            
        return _roleDao;
    }

    /* ---------------------------------------------------------------------------- */
    /* getSkRoleTreeDao:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Create the shared dao on first reference.
     * 
     * @return the dao
     * @throws TapisException on error
     */
    protected static SkRoleTreeDao getSkRoleTreeDao() 
     throws TapisException
    {
        // Avoid synchronizing exception for initialization.
        if (_roleTreeDao == null) 
            synchronized (BaseImpl.class) {
                if (_roleTreeDao == null) _roleTreeDao = new SkRoleTreeDao();
           }
            
        return _roleTreeDao;
    }

    /* ---------------------------------------------------------------------------- */
    /* getSkRolePermissionDao:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** Create the shared dao on first reference.
     * 
     * @return the dao
     * @throws TapisException on error
     */
    protected static SkRolePermissionDao getSkRolePermissionDao() 
     throws TapisException
    {
        // Avoid synchronizing exception for initialization.
        if (_rolePermissionDao == null) 
            synchronized (BaseImpl.class) {
                if (_rolePermissionDao == null) _rolePermissionDao = new SkRolePermissionDao();
           }
            
        return _rolePermissionDao;
    }

    /* ---------------------------------------------------------------------------- */
    /* getSkUserRoleDao:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Create the shared dao on first reference.
     * 
     * @return the dao
     * @throws TapisException on error
     */
    protected static SkUserRoleDao getSkUserRoleDao() 
     throws TapisException
    {
        // Avoid synchronizing exception for initialization.
        if (_userRoleDao == null) 
            synchronized (BaseImpl.class) {
                if (_userRoleDao == null) _userRoleDao = new SkUserRoleDao();
           }
            
        return _userRoleDao;
    }

    /* ---------------------------------------------------------------------------- */
    /* getRoleId:                                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Return the ID of a role given its tenant and name.  An exception is thrown
     * if for any reason an ID could not be retrieved.
     * 
     * @param tenant the role's tenant
     * @param roleName the role's name
     * @return the role's id
     * @throws TapisException if the id was not retrieved
     */
    protected int getRoleId(String tenant, String roleName) 
     throws TapisException, TapisNotFoundException
    {
        // Get the dao.
        SkRoleDao roleDao = null;
        try {roleDao = getSkRoleDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "roles");
                _log.error(msg, e);
                throw new TapisException(msg);
            }
        
        // Get the role id.
        Integer roleId = null;
        try {roleId = roleDao.getRoleId(tenant, roleName);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_GET_ROLE_ID_ERROR", roleName,
                                             tenant, e.getMessage());
                _log.error(msg, e);
                throw new TapisException(msg);
            }
        
        // Make sure we found the role.
        if (roleId == null) {
            String msg = MsgUtils.getMsg("SK_ROLE_NOT_FOUND", tenant, roleName);
            _log.error(msg);
            throw new TapisNotFoundException(msg, roleName);
        }
        
        return roleId;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getUserDefaultRolename:                                                      */
    /* ---------------------------------------------------------------------------- */
    public String getUserDefaultRolename(String user)
    {return USER_DEFAULT_ROLE_PREFIX + user;}
}

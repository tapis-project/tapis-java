package edu.utexas.tacc.tapis.security.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleTreeDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkUserRoleDao;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;

class AbstractResource 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(AbstractResource.class);

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // We share all dao's among all instances of this class.
    private static SkRoleDao           _roleDao;
    private static SkRoleTreeDao       _roleTreeDao;
    private static SkRolePermissionDao _roleTreePermissionDao;
    private static SkUserRoleDao       _userRoleDao;
    
    // Role name validator.  Require names to start with alphabetic characters and 
    // be followed by zero or more alphanumeric characters and underscores.
    private static final Pattern _namePattern = Pattern.compile("^\\p{Alpha}(\\p{Alnum}|_)*");
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getPayload:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Get the request's json payload and use it to populate a request-specified 
     * object.
     * 
     * @param <T> the result type
     * @param payloadStream the request payload stream returned by jaxrs
     * @param schemaFile the schema file that validates the request json
     * @param classOfT the class of the result object
     * @return the object that received the payload
     * @throws TapisException on error
     */
    protected <T> T getPayload(InputStream payloadStream, String schemaFile, Class<T> classOfT)
     throws TapisException
    {
        // There better be a payload.
        String json = null;
        try {json = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "security", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
          }
        
        // Create validator specification.
        JsonValidatorSpec spec = new JsonValidatorSpec(json, schemaFile);
        
        // Make sure the json conforms to the expected schema.
        try {JsonValidator.validate(spec);}
          catch (TapisJSONException e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
          }

        // Populate the result object with the json values.
        T payload = null;
        try {payload = TapisGsonUtils.getGson().fromJson(json, classOfT);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());            
                _log.error(msg, e);
                throw new TapisException(msg, e);
            }
        
       return payload; 
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
     throws TapisException
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
            throw new TapisException(msg);
        }
        
        return roleId;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* allNullOrNot:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** Return true if either both parameters are null or both are not null.
     * Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @return true if both parameters have the same nullity, false otherwise
     */
    protected boolean allNullOrNot(Object o1, Object o2)
    {
        if (o1 == null && o2 == null) return true;
        if (o1 != null && o2 != null) return true;
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* allNullOrNot:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** Return true if either all parameters are null or all are not null.
     * Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @param o3 yet another object
     * @return true if all parameters have the same nullity, false otherwise
     */
    protected boolean allNullOrNot(Object o1, Object o2, Object o3)
    {
        if (o1 == null && o2 == null && o3 == null) return true;
        if (o1 != null && o2 != null && o3 != null) return true;
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* allNull:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Return true if both parameters are null. Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @return true if both parameters are null, false otherwise
     */
    protected boolean allNull(Object o1, Object o2)
    {
        if (o1 == null && o2 == null) return true;
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* allNull:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Return true if all parameters are null. Otherwise, return false.
     * 
     * @param o1 any object
     * @param o2 any other object
     * @param o3 yet another object
     * @return true if all parameters are null, false otherwise
     */
    protected boolean allNull(Object o1, Object o2, Object o3)
    {
        if (o1 == null && o2 == null && o3 == null) return true;
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* checkTenantUser:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Check that we have value tenant id and user names in the threadlocal cache.
     * Return null if the check succeed, otherwise return the error response.
     * 
     * @param prettyPrint whether to pretty print the response or not
     * @return null on success, a response on error
     */
    protected Response checkTenantUser(TapisThreadContext threadContext, boolean prettyPrint)
    {
        // Get the thread local context and validate context parameters.  The
        // tenantId and user are set in the jaxrc filter classes that process
        // each request before processing methods are invoked.
        if (!threadContext.validate()) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
          _log.error(msg);
          return Response.status(Status.BAD_REQUEST).
              entity(RestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
        
        // Success
        return null;
    }

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
            synchronized (AbstractResource.class) {
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
            synchronized (AbstractResource.class) {
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
        if (_roleTreePermissionDao == null) 
            synchronized (AbstractResource.class) {
                if (_roleTreePermissionDao == null) _roleTreePermissionDao = new SkRolePermissionDao();
           }
            
        return _roleTreePermissionDao;
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
            synchronized (AbstractResource.class) {
                if (_userRoleDao == null) _userRoleDao = new SkUserRoleDao();
           }
            
        return _userRoleDao;
    }

    /* ---------------------------------------------------------------------------- */
    /* isValidName:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Check a candidate name against the name regex.
     * 
     * @param name the name to validate
     * @return true if matches regex, false otherwise
     */
    protected boolean isValidName(String name)
    {
        if (name == null) return false;
        return _namePattern.matcher(name).matches();
    }

}

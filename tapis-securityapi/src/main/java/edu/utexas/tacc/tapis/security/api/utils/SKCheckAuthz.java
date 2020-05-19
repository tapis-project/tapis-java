package edu.utexas.tacc.tapis.security.api.utils;

import java.util.ArrayList;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl.AuthOperation;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext.AccountType;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

public final class SKCheckAuthz 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SKCheckAuthz.class);

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Constructor parameters.
    private final String _reqTenant;
    private final String _reqUser;
    private final TapisThreadContext _threadContext;
    private final ArrayList<String> _failedChecks = new ArrayList<>();;
    
    // Checks.
    private boolean _checkMatchesJwtIdentity;
    private boolean _checkIsAdmin;
    private boolean _checkServiceAllowed;
    private boolean _checkServiceOBO;
    
    // Secret path checks.
    private String  _checkSecretDBCred;
    private String  _checkSecretJWTSigning;
    private String  _checkSecretService;
    private String  _checkSecretSystem;
    private String  _checkSecretUser;
    
    // Roles that the jwt user@tenant has some level of access.
    private ArrayList<String> _requiredRoles;
    private ArrayList<String> _ownedRoles;

    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** For performance reasons, we just assume each parameter is non-null. We access
     * all JWT values through the local thread context. It is assumed that the 
     * AbstractResource.checkTenantUser() has already been called, which guarantees
     * a valid context. 
     */
    private SKCheckAuthz(String reqTenant, String reqUser)
    {
        // It's the caller's responsibility to make 
        // sure these parameters are non-null.
        _reqTenant   = reqTenant;
        _reqUser     = reqUser;
        
        // Get the jwt information.
        _threadContext = TapisThreadLocal.tapisThreadContext.get();
    }
    
    /* **************************************************************************** */
    /*                                Accessors                                     */
    /* **************************************************************************** */
    public SKCheckAuthz setCheckMatchesJwtIdentity() {_checkMatchesJwtIdentity = true; return this;}
    public SKCheckAuthz setCheckIsAdmin() {_checkIsAdmin = true; return this;}
    public SKCheckAuthz setCheckServiceAllowed() {_checkServiceAllowed = true; return this;}
    public SKCheckAuthz setCheckServiceOBO() {_checkServiceOBO = true; return this;}
    public SKCheckAuthz setCheckSecretDBCred(String path) {_checkSecretDBCred = path; return this;}
    public SKCheckAuthz setCheckSecretJWTSigning(String path) {_checkSecretJWTSigning = path; return this;}
    public SKCheckAuthz setCheckSecretService(String path) {_checkSecretService = path; return this;}
    public SKCheckAuthz setCheckSecretSystem(String path) {_checkSecretSystem = path; return this;}
    public SKCheckAuthz setCheckSecretUser(String path) {_checkSecretUser = path; return this;}
    
    // Role lists.
    public SKCheckAuthz addRequiredRole(String roleName) 
    {
        if (_requiredRoles == null) _requiredRoles = new ArrayList<>();
        _requiredRoles.add(roleName);
        return this;
    }
    public SKCheckAuthz addOwnedRole(String roleName) 
    {
        if (_ownedRoles == null) _ownedRoles = new ArrayList<>();
        _ownedRoles.add(roleName);
        return this;
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* configure:                                                                   */
    /* ---------------------------------------------------------------------------- */
    public static SKCheckAuthz configure(String reqTenant, String reqUser)
    {return new SKCheckAuthz(reqTenant, reqUser);}
    
    /* ---------------------------------------------------------------------------- */
    /* check:                                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Run the check as it's currently configured.  The checks are treated as a 
     * single disjunctive expression in which the first passing test causes true
     * to be returned and the remaining tests skipped.  The tests are generally
     * ordered from least expensive to most expensive.
     * 
     * @return null if authorization matches, otherwise return an error message.
     */
    public String check()
    {
        // Validate the thread context and allowable tenant on service tokens.
        String validateMsg = validateTenantContext();
        if (validateMsg != null) return validateMsg;
        
        // Try to order checks by least expensive and most often used.
        if (_checkMatchesJwtIdentity && checkMatchesJwtIdentity()) return null;
        if (_checkIsAdmin && checkIsAdmin()) return null;
        if (_checkServiceAllowed && checkServiceAllowed()) return null;
        if (_checkServiceOBO && checkServiceOBO()) return null;
        if (_requiredRoles != null && checkRequiredRoles()) return null;
        if (_ownedRoles != null && checkOwnedRoles()) return null;
        
        // Returns a non-null error message only if at least one check failed.
        return makeFailureMessage();
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* validateTenantContext:                                                       */
    /* ---------------------------------------------------------------------------- */
    private String validateTenantContext()
    {
        // Get the thread local context and validate context parameters.  The
        // tenantId and user are set in the jaxrc filter classes that process
        // each request before processing methods are invoked.
        if (!_threadContext.validate()) 
            return MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
        
        // Collect some context information.
        String jwtTenant = _threadContext.getJwtTenantId();
        AccountType accountType = _threadContext.getAccountType();
        
        // Service accounts are allowed more latitude than user accounts.
        // Specifically, they can specify tenants other than that in their jwt.
        if (accountType == AccountType.service) {
            boolean allowedTenant;
            try {allowedTenant = TapisRestUtils.isAllowedTenant(jwtTenant, _reqTenant);}
                catch (Exception e) {
                    String jwtUser = _threadContext.getJwtUser();
                    return MsgUtils.getMsg("TAPIS_SECURITY_ALLOWABLE_TENANT_ERROR", 
                                                 jwtUser, jwtTenant, _reqTenant);
                }
            
            // Can the new tenant id be used by the jwt tenant?
            if (!allowedTenant) {
                String jwtUser = _threadContext.getJwtUser();
                return MsgUtils.getMsg("TAPIS_SECURITY_TENANT_NOT_ALLOWED", 
                                       jwtUser, jwtTenant, _reqTenant);
            }
        } else {
            // User tokens require exact tenant matches.
            if (!jwtTenant.equals(_reqTenant)) 
                return MsgUtils.getMsg("SK_UNEXPECTED_TENANT_VALUE", 
                                       jwtTenant, _reqTenant, accountType.name());
        }

        // Success.
        return null;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* checkMatchesJwtIdentity:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Check that the jwt identity exactly matches the request tenant and user.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkMatchesJwtIdentity()
    {
        // See if the jwt and request user@tenant are the same.
        if (_threadContext.getJwtTenantId().equals(_reqTenant) &&
            _threadContext.getJwtUser().equals(_reqUser)) return true;
        
        _failedChecks.add("MatchesJwtIdentity");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkIsUser:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Check that the jwt identity represents a user.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkIsUser()
    {
        // See if the jwt and request user@tenant are the same.
        if (_threadContext.getAccountType() == AccountType.user) return true;
        _failedChecks.add("IsUser");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkIsService:                                                              */
    /* ---------------------------------------------------------------------------- */
    /** Check that the jwt identity represents a service.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkIsService()
    {
        // See if the jwt and request user@tenant are the same.
        if (_threadContext.getAccountType() == AccountType.service) return true;
        _failedChecks.add("IsService");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkIsAdmin:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** Check that the jwt identity is an administrator.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkIsAdmin()
    {
        // See if the jwt user has admin privileges.
        boolean authorized = false;
        try {
            var userImpl = UserImpl.getInstance();
            authorized = userImpl.hasRole(_threadContext.getJwtTenantId(),
                                          _threadContext.getJwtUser(), 
                                          new String[] {UserImpl.ADMIN_ROLE_NAME}, 
                                          AuthOperation.ANY);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                         _threadContext.getJwtTenantId(), 
                                         _threadContext.getJwtUser(), e.getMessage());
            _log.error(msg, e);
        }
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("IsAdmin");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkServiceAllowed:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Check that the jwt identity is for a service can act on behalf of the tenant in
     * the request.  This effectively authorizes any such service to perform any action 
     * in the tenant.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkServiceAllowed()
    {
        // Start pessimistically.
        boolean allowedTenant = false;
        
        // First make sure we are dealing with a service.
        if (_threadContext.getAccountType() == AccountType.service) {
            // Service accounts are allowed more latitude than user accounts.
            // Specifically, they can specify tenants other than that in their jwt. 
            String jwtTenant = _threadContext.getJwtTenantId();
            try {allowedTenant = TapisRestUtils.isAllowedTenant(jwtTenant, _reqTenant);}
                catch (Exception e) {
                    String jwtUser = _threadContext.getJwtUser();
                    String msg = MsgUtils.getMsg("TAPIS_SECURITY_ALLOWABLE_TENANT_ERROR", 
                                                 jwtUser, jwtTenant, _reqTenant);
                    _log.error(msg, e);
                }
        }
        
        // What happened?
        if (allowedTenant) return true;
        _failedChecks.add("AllowedService");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkServiceOBO:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Check that the identity provided on the OBO headers is one on behalf of whom
     * this service is acting.  First the service's tenant as expressed in the jwt 
     * must be allowed to act on behalf of the request tenant.  In addition, the 
     * request user and tenant must match the OBO user and tenant.  This effectively 
     * authorizes the service to perform any action on behalf of the OBO identity.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkServiceOBO()
    {
        // Only services need apply.
        if (_threadContext.getAccountType() != AccountType.service) return false;
        
        // The service's tenant must be allowed to act on behalf of the request tenant.
        if (!checkServiceAllowed()) return false;
        
        // Start pessimistically.
        boolean allowedIdentity;
        
        // Restrict the request identity to be the OBO identity.
        if (_reqTenant.equals(_threadContext.getOboTenantId()) &&
            _reqUser.equals(_threadContext.getOboUser())) 
            allowedIdentity = true;
          else allowedIdentity = false;
        
        // What happened?
        if (allowedIdentity) return true;
        _failedChecks.add("OnBehalfOfService");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkIsRoleOwner:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Checks the user identified by the jwt is the role owner.  If more than one
     * role is in the _ownedRoles list, all roles must be owned by the jwt user. 
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkOwnedRoles()
    {
        // Idiot check prohibits no roles from implying authorization. 
        if (_ownedRoles == null || _ownedRoles.isEmpty()) return false;
        
        // Start optimistically.
        boolean authorized = true;
        try {
            var roleImpl = RoleImpl.getInstance();
            for (String roleName : _ownedRoles) {
               var skRole = roleImpl.getRoleByName(_threadContext.getJwtTenantId(), roleName);
               if (skRole == null || !_threadContext.getJwtUser().equals(skRole.getCreatedby())) {
                   // The role doesn't exist or request user is not its owner.
                   authorized = false;
                   break;
               }
            }
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                         _threadContext.getJwtTenantId(), 
                                         _threadContext.getJwtUser(), e.getMessage());
            _log.error(msg, e);
            authorized = false;
        }
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("OwnedRoles");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* checkRequiredRoles:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Check that all required roles are assigned to the jwt identity.  This method
     * should only be called when at least one required role has been specified. 
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkRequiredRoles()
    {
        // Idiot check prohibits no roles from implying authorization. 
        if (_requiredRoles == null || _requiredRoles.isEmpty()) return false;
        
        // Start pessimistically.
        boolean authorized = false;
        try {
            String[] roles = new String[_requiredRoles.size()];
            roles = _requiredRoles.toArray(roles);
            var userImpl = UserImpl.getInstance();
            authorized = userImpl.hasRole(_threadContext.getJwtTenantId(),
                                          _threadContext.getJwtUser(), 
                                          roles, AuthOperation.ALL);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                         _threadContext.getJwtTenantId(), 
                                         _threadContext.getJwtUser(), e.getMessage());
            _log.error(msg, e);
        }
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("RequiredRoles");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* makeFailureMessage:                                                          */
    /* ---------------------------------------------------------------------------- */
    private String makeFailureMessage()
    {
        // If no checks were specified, nothing failed
        // and authorization is successful.
        if (_failedChecks.isEmpty()) return null;
        
        // Put the list of failed checks in the error message.  
        // The caller is expected to log the message.
        String s = _failedChecks.stream().collect(Collectors.joining(", "));
        String msg = MsgUtils.getMsg("SK_API_AUTHORIZATION_FAILED", 
                                     _reqTenant, _reqUser, s);
        return msg;
    }
}

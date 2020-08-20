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
import edu.utexas.tacc.tapis.security.config.SkConstants;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.TapisConstants;
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
    private final String                _reqTenant;  // can be null
    private final String                _reqUser;    // can be null
    
    private final String                _jwtTenant;  // never null
    private final String                _jwtUser;    // never null
    private final SecretPathMapperParms _secretPathParms;
    private final TapisThreadContext    _threadContext;
    private final ArrayList<String>     _failedChecks = new ArrayList<>();
    
    // Identity checks.
    private boolean _checkMatchesJwtIdentity;
    private boolean _checkIsAdmin;
    private boolean _checkIsService;
    private boolean _checkIsFilesService;
    
    // Secrets checks.
    private boolean _checkSecrets;
    private boolean _validatePassword;
    
    // Roles that the jwt user@tenant has some level of access.
    private ArrayList<String> _requiredRoles;
    private ArrayList<String> _ownedRoles;
    
    // Prevention switches.
    private boolean _preventAdminRole;
    private String  _preventAdminRoleName;
    private boolean _preventForeignTenantUpdate;

    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** For performance reasons, we just assume each parameter is non-null. We access
     * all JWT values through the local thread context. The first check performed is
     * validateTenantContext(), which guarantees a valid tenant context.  It's never
     * valid to provide a null or empty request tenant; basic checking does not require 
     * a valid request user. 
     */
    private SKCheckAuthz(String reqTenant, String reqUser,
                         SecretPathMapperParms secretPathParms)
    {
        // Null parameters are detected before first use. 
        _reqTenant        = reqTenant;
        _reqUser          = reqUser;
        _secretPathParms  = secretPathParms;
        
        // Get the jwt information.
        _threadContext = TapisThreadLocal.tapisThreadContext.get();
        _jwtUser   = _threadContext.getJwtUser();
        _jwtTenant = _threadContext.getJwtTenantId();
    }
    
    /* **************************************************************************** */
    /*                                Accessors                                     */
    /* **************************************************************************** */
    // Roles and user checks.
    public SKCheckAuthz setCheckMatchesJwtIdentity() {_checkMatchesJwtIdentity = true; return this;}
    public SKCheckAuthz setCheckIsAdmin()    {_checkIsAdmin = true; return this;}
    public SKCheckAuthz setCheckIsService()  {_checkIsService = true; return this;}
    public SKCheckAuthz setCheckIsFilesService() {_checkIsFilesService = true; return this;}
    
    // Secrets checks.
    public SKCheckAuthz setCheckSecrets() {_checkSecrets = true; return this;}
    public SKCheckAuthz setValidatePassword() {_validatePassword = true; return this;}
    
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
    
    // Prevention switches.
    public SKCheckAuthz setPreventForeignTenantUpdate() {_preventForeignTenantUpdate = true; return this;}
    public SKCheckAuthz setPreventAdminRole(String roleName) 
    {
    	_preventAdminRole = true; 
    	_preventAdminRoleName = roleName;
    	return this;
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* configure:                                                                   */
    /* ---------------------------------------------------------------------------- */
    public static SKCheckAuthz configure(String reqTenant, String reqUser,
                                         SecretPathMapperParms secretPathParms)
    {return new SKCheckAuthz(reqTenant, reqUser, secretPathParms);}
    
    /* ---------------------------------------------------------------------------- */
    /* configure:                                                                   */
    /* ---------------------------------------------------------------------------- */
    public static SKCheckAuthz configure(String reqTenant, String reqUser)
    {return new SKCheckAuthz(reqTenant, reqUser, null);}
    
    /* ---------------------------------------------------------------------------- */
    /* check:                                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Return the jaxrs response if an authorization check failed.  This method 
     * should be called before prevent() so that basic validation occurs.
     * 
     * @return null for authorized, a response object for failed authorization
     */
    public Response check(boolean prettyPrint)
    {
        // Perform the actual checks.
        String emsg = checkMsg();              // At least 1 check must succeed.
        if (emsg == null) emsg = preventMsg(); // All checks must succeed.
        if (emsg == null) return null;         // Success.
        
        // Determine the http failure code.
        Status status;
        if (!_failedChecks.isEmpty()) status = Status.UNAUTHORIZED;
        else if (emsg.startsWith("TAPIS_SECURITY_ALLOWABLE_TENANT_ERROR"))
            status = Status.INTERNAL_SERVER_ERROR;
        else status = Status.BAD_REQUEST;
        
        // Return an error response.
        return Response.status(status).
          entity(TapisRestUtils.createErrorResponse(emsg, prettyPrint)).build();
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* checkMsg:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Run the check as it's currently configured.  The checks are treated as a 
     * single disjunctive expression in which the first passing test causes true
     * to be returned and the remaining tests are skipped.  The tests are generally
     * ordered from least expensive to most expensive.
     * 
     * Note: Since _reqUser can legitimately be null on some API calls, every use of 
     * _reqUser needs to be protected by an explicit null check.
     * 
     * @return null if authorization matches, otherwise return an error message.
     */
    private String checkMsg()
    {
        // Validate the thread context and allowable tenant on service tokens.
        // Request tenant null check performed in called method.
        String validateMsg = validateRequestTenantContext();
        if (validateMsg != null) return validateMsg;
        
        // Try to order checks by least expensive and most often used.
        // This sequencing of checks implements a logical disjunction
        // with "short-circuiting" behavior.
        if (_checkIsService && checkIsService()) return null;
        if (_checkMatchesJwtIdentity && checkMatchesJwtIdentity()) return null;
        if (_checkIsAdmin && checkIsAdmin()) return null;
        if (_checkIsFilesService && checkIsFilesService()) return null;
        
        if (_ownedRoles != null && checkOwnedRoles()) return null;
        if (_requiredRoles != null && checkRequiredRoles()) return null;
        
        if (_checkSecrets && checkSecrets()) return null;
        if (_validatePassword && validatePassword()) return null;
        
        // Returns a non-null error message only if at least one check failed.
        return makeFailureMessage();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* preventMsg:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Run the enabled prevention routines.  All enabled prevention constraints muse
     * pass--a failure of any one of them causes the request to be aborted.
     * 
     * @return null if there are no problems, otherwise return an error message.
     */
    private String preventMsg()
    {
    	// All enabled prevention routines must succeed for overall success. 
    	if (_preventAdminRole) {
    		String errorMsg = preventAdminRole();
    		if (errorMsg != null) return errorMsg;
    	}
    	
    	// Limit updates to allowable tenants.
    	if (_preventForeignTenantUpdate) {
    		String errorMsg = preventForeignTenantUpdate();
    		if (errorMsg != null) return errorMsg;
    	}
    	
    	// No problems found.
    	return null;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateRequestTenantContext:                                                */
    /* ---------------------------------------------------------------------------- */
    /** This method first validates the thread context, which allows all subsequent 
     * checks to be performed without risk of NPEs on threadlocal data.  
     * 
     * For service tokens, we check that the jwt tenant is allowed to be used on 
     * behalf of the request tenant.  For user tokens, we check that the jwt and 
     * request tenants are the same.
     * 
     * If the _reqTenant is set, then consistency checks between that tenant value
     * and the JWT tenant value are performed.  If _reqTenant is not set, then
     * the request does not require that value in its payload or among its query
     * parameters, so no consistency checks need to be performed.
     * 
     * This method does not use the _reqUser field, so it can always be null.
     * 
     * @return null for success, an error message on failure
     */
    private String validateRequestTenantContext()
    {
        // Get the thread local context and validate context parameters.  The
        // tenantId and user are set in the jaxrc filter classes that process
        // each request before processing methods are invoked.
        if (!_threadContext.validate()) {
            var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
            _log.error(msg);
            return msg;
        }
        
        // If a tenant value is sent in the request, then make sure the requestor can
        // act upon that tenant's resources.
        if (_reqTenant == null) return null; // success
        
        // Service accounts are allowed more latitude than user accounts.
        // Specifically, they can specify tenants other than that in their jwt.
        AccountType accountType = _threadContext.getAccountType();
        if (accountType == AccountType.user) {
            // User tokens require exact tenant matches.
            if (!_jwtTenant.equals(_reqTenant)) {
                var msg = MsgUtils.getMsg("SK_UNEXPECTED_TENANT_VALUE", _jwtUser,
                		                  _jwtTenant, _reqTenant, accountType.name());
                _log.error(msg);
                return msg;
            }
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
        // Make sure the request user has been assigned for this check.
        if (_reqUser == null) {
            var msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "checkMatchesJwtIdentity", "_reqUser");
            _log.error(msg);
            _failedChecks.add("MatchesJwtIdentity");
            return false;
        }
            
        // See if the jwt and request user@tenant are the same.
        if (_jwtTenant.equals(_reqTenant) && _jwtUser.equals(_reqUser)) return true;
        
        // Record failure.
        _failedChecks.add("MatchesJwtIdentity");
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
            authorized = userImpl.hasRole(_jwtTenant, _jwtUser, 
                                          new String[] {UserImpl.ADMIN_ROLE_NAME}, 
                                          AuthOperation.ANY);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
            		                     _jwtTenant, _jwtUser, e.getMessage());
            _log.error(msg, e);
        }
        
        // Make sure the jwt identity is an admin in the request tenant.
        // We already checked this for user jwt's in context validation, 
        // but now we also check it for service jwt's.
        if (authorized && _reqTenant != null) {
            if (!_jwtTenant.equals(_reqTenant)) {
                var msg = MsgUtils.getMsg("SK_UNEXPECTED_TENANT_VALUE", _jwtUser,
                		                  _jwtTenant, _reqTenant, 
                		                  _threadContext.getAccountType().name());
                _log.error(msg);
                authorized = false;
            }
        }
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("IsAdmin");
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
    /* checkIsFilesService:                                                         */
    /* ---------------------------------------------------------------------------- */
    private boolean checkIsFilesService()
    {
        // Start pessimistically.
        boolean authorized = false;
        
        // Are the path parms configured and is the caller the tokens service?  
        if (_threadContext.getAccountType() == AccountType.service &&
            _threadContext.getJwtUser().equals(TapisConstants.SERVICE_NAME_FILES))
           authorized = true;
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("IsFilesService");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* checkOwnedRoles:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Checks the user identified in the request JWT is the role owner.  If more than
     * one role is in the _ownedRoles list, all roles must be owned by the request user. 
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkOwnedRoles()
    {
        // Idiot check prohibits no roles from implying authorization. 
        if (_ownedRoles == null || _ownedRoles.isEmpty()) return false;
        
        // Make sure the request tenant has been assigned for this check.
        // In this context, the request tenant represents the tenant of 
        // all the roles being checked.
        if (_reqTenant == null) {
            var msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "checkOwnedRoles", "_reqTenant");
            _log.error(msg);
            _failedChecks.add("OwnedRoles");
            return false;
        }
        
        // Start optimistically.
        boolean authorized = true;
        try {
        	// This block checks the role owner identity.
            var roleImpl = RoleImpl.getInstance();
            for (String roleName : _ownedRoles) {
                // The request and role tenants are guaranteed to be the same
            	// because we use the request tenant in the retrieval.
                var skRole = roleImpl.getRoleByName(_reqTenant, roleName);
                
                // Bad news.
                if (skRole == null) {authorized = false; break;}
                
                // Make sure the user identified in the JWT is owner of the role.
                // Note that the owner can actually be in a different tenant than
                // than the role, but the parent and child roles must be in the
                // same tenant (_reqTenant).
                if (!_jwtUser.equals(skRole.getOwner()) || 
                	!_jwtTenant.equals(skRole.getOwnerTenant())) 
                {
                	authorized = false; break;
                }
            }
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                         _jwtTenant, _jwtUser, e.getMessage());
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
            authorized = userImpl.hasRole(_jwtTenant, _jwtUser, 
                                          roles, AuthOperation.ALL);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
            		                     _jwtTenant, _jwtUser, e.getMessage());
            _log.error(msg, e);
        }
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("RequiredRoles");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkSecrets:                                                                */
    /* ---------------------------------------------------------------------------- */
    private boolean checkSecrets()
    {
        // Don't authorize if secret information has not been provided.
        if (_secretPathParms == null) {
            _log.error(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "checkSecrets", "_secretPathParms"));
            _failedChecks.add("Secrets");
            return false;
        }
        
        // Processing depends on the secret type.
        boolean authorized = false;
        switch (_secretPathParms.getSecretType())
        {
            case System:
                authorized = secretCheckIsSystemsService();
            break;
            
            case DBCredential:
                authorized = checkIsService();
            break;
            
            case ServicePwd:
                authorized = secretCheckServicePwd();
            break;
            
            case JWTSigning:
                authorized = secretCheckIsTokensService();
            break;
            
            case User:
                authorized = secretCheckSecretUser();
            break;
            
            default:
                // This should never happen as long as all cases are covered.
                var secretTypes = new ArrayList<String>();
                for (SecretType t : SecretType.values()) secretTypes.add(t.name());
                String msg =  MsgUtils.getMsg("SK_VAULT_INVALID_SECRET_TYPE",
                                              _secretPathParms.getSecretType().name(), secretTypes);
                _log.error(msg);
            break;
        }
        
        return authorized;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* secretCheckIsSystemsService:                                                 */
    /* ---------------------------------------------------------------------------- */
    private boolean secretCheckIsSystemsService()
    {
        // Start pessimistically.
        boolean authorized = false;
        
        // Are the path parms configured and is the caller the tokens service?  
        if (_threadContext.getAccountType() == AccountType.service &&
            _jwtUser.equals(TapisConstants.SERVICE_NAME_SYSTEMS)) 
           authorized = true;
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("IsSystemsService");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* secretCheckIsTokensService:                                                  */
    /* ---------------------------------------------------------------------------- */
    private boolean secretCheckIsTokensService()
    {
        // Are the path parms configured and is the caller the tokens service?  
        if (_threadContext.getAccountType() == AccountType.service &&
            _jwtUser.equals(TapisConstants.SERVICE_NAME_TOKENS)) 
           return true;
        
        // Failure.
        _failedChecks.add("IsTokensService");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* secretCheckServicePwd:                                                       */
    /* ---------------------------------------------------------------------------- */
    private boolean secretCheckServicePwd()
    {return secretCheckServiceRequestIdentity("ServicePassword");}
    
    /* ---------------------------------------------------------------------------- */
    /* validatePassword:                                                            */
    /* ---------------------------------------------------------------------------- */
    private boolean validatePassword()
    {
        // Are the path parms configured and is the caller the tokens service?
    	// We report a failure here only if the next check also fails.
        if (_threadContext.getAccountType() == AccountType.service &&
            _jwtUser.equals(TapisConstants.SERVICE_NAME_TOKENS)) 
           return true;
        
        // A service can validate its own password. This method records its own failure.
        if (secretCheckServiceRequestIdentity("ValidatePassword")) return true;
        
        // Neither check passed so we add the first check's failure to the record.
        _failedChecks.add("IsTokensService");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* secretCheckServiceRequestIdentity:                                           */
    /* ---------------------------------------------------------------------------- */
    private boolean secretCheckServiceRequestIdentity(String checkName)
    {
        // Start pessimistically.
        boolean authorized = false;
        
        // We need to be a specific service.
        if (_reqUser != null &&
            _threadContext.getAccountType() == AccountType.service &&
            _jwtTenant.equals(_reqTenant) &&
            _jwtUser.equals(_reqUser))
            authorized = true;
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add(checkName);
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* secretCheckSecretUser:                                                       */
    /* ---------------------------------------------------------------------------- */
    private boolean secretCheckSecretUser()
    {
        // Start pessimistically.
        boolean authorized = false;
        
        // We need to be a specific user or service.
        if (_reqUser != null &&
            _jwtTenant.equals(_reqTenant) &&
            _jwtUser.equals(_reqUser))
            authorized = true;
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("UserSecret");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* preventAdminRole:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Prevent the tenant admin role from being specified on certain APIs.
     * 
     * @return null if ok, an error message if the check fails
     */
    private String preventAdminRole()
    {
        // Blacklist certain administrative roles.  We call this after the normal authz
        // checking so that we know we have a validated threadlocal.
        if (SkConstants.ADMIN_ROLE_NAME.equals(_preventAdminRoleName)) {
            return MsgUtils.getMsg("SK_TENANT_ADMIN_ROLE_ERROR", 
            		               _jwtTenant, _jwtUser);
        }
        
    	// No problem.
    	return null;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* preventForeignTenantUpdate:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Prevent services that do not have allowable-tenant rights in a request tenant 
     * from creating or updating that resources in that tenant.
     * 
     * @return null if ok, an error message if the check fails
     */
    private String preventForeignTenantUpdate()
    {
        // This shouldn't happen because this check should only be enabled
    	// on requests that require a request tenant parameter.
        if (_reqTenant == null) return null; // success
        
        // Initial validation already covers the user jwt case.
        if (_threadContext.getAccountType() == AccountType.user) return null;
        
        // Service accounts are allowed more latitude than user accounts.
        // Specifically, they can specify tenants other than that in their jwt.
        // Make sure the service's jwt covers the request tenant.
        boolean allowedTenant;
        try {allowedTenant = TapisRestUtils.isAllowedTenant(_jwtTenant, _reqTenant);}
            catch (Exception e) {
                String jwtUser = _threadContext.getJwtUser();
                var msg = MsgUtils.getMsg("TAPIS_SECURITY_ALLOWABLE_TENANT_ERROR", 
                                          jwtUser, _jwtTenant, _reqTenant);
                _log.error(msg, e);
                return msg;
            }
            
        // Can the new tenant id be used by the jwt tenant?
        if (!allowedTenant) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_TENANT_NOT_ALLOWED", 
                                         _jwtUser, _jwtTenant, _reqTenant);
            _log.error(msg);
            _failedChecks.add("preventForeignTenantUpdate"); // triggers unauthorized code
            return msg;
        }

    	// Success.
    	return null;
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
                                     _reqTenant, _reqUser, 
                                     _jwtTenant,
                                     _jwtUser,
                                     _threadContext.getOboTenantId(),
                                     _threadContext.getOboUser(),
                                     _threadContext.getAccountType(),
                                     s);
        _log.error(msg);
        return msg;
    }
}

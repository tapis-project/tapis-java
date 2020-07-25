package edu.utexas.tacc.tapis.security.api.utils;

import java.util.ArrayList;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl.AuthOperation;
import edu.utexas.tacc.tapis.security.config.SkConstants;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
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
    
    // Service names.
    private static final String TOKENS_SERVICE_NAME  = "tokens";
    private static final String SYSTEMS_SERVICE_NAME = "systems";

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Constructor parameters.
    private final String                _reqTenant;  // never null
    private final String                _reqUser;    // can be null
    private final SecretPathMapperParms _secretPathParms;
    private final TapisThreadContext    _threadContext;
    private final ArrayList<String>     _failedChecks = new ArrayList<>();
    private final ArrayList<String>     _provisionalFailedChecks = new ArrayList<>();
    
    // Identity checks.
    private boolean _checkMatchesJwtIdentity;
    private boolean _checkMatchesOBOIdentity;
    private boolean _checkIsAdmin;
    private boolean _checkIsOBOAdmin;
    private boolean _checkIsService;
    
    // Secrets checks.
    private boolean _checkSecrets;
    private boolean _validatePassword;
    
    // Roles that the jwt user@tenant has some level of access.
    private ArrayList<String> _requiredRoles;
    private ArrayList<String> _ownedRoles;
    
    // Prevention switches.
    private boolean _preventAdminRole;
    private String  _preventAdminRoleName;

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
    }
    
    /* **************************************************************************** */
    /*                                Accessors                                     */
    /* **************************************************************************** */
    // Roles and user checks.
    public SKCheckAuthz setCheckMatchesJwtIdentity() {_checkMatchesJwtIdentity = true; return this;}
    public SKCheckAuthz setCheckMatchesOBOIdentity() {_checkMatchesOBOIdentity = true; return this;}
    public SKCheckAuthz setCheckIsAdmin()    {_checkIsAdmin = true; return this;}
    public SKCheckAuthz setCheckIsOBOAdmin() {_checkIsOBOAdmin = true; return this;}
    public SKCheckAuthz setCheckIsService()  {_checkIsService = true; return this;}
    
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
    public String checkMsg()
    {
        // Validate the thread context and allowable tenant on service tokens.
        // Request tenant null check performed in called method.
        String validateMsg = validateRequestTenantContext();
        if (validateMsg != null) return validateMsg;
        
        // Try to order checks by least expensive and most often used.
        // This sequencing of checks implements a logical disjunction
        // with "short-circuiting" behavior.
        if (_checkIsService && checkIsService()) return null;
        if (_checkMatchesJwtIdentity && checkMatchesJwtIdentity(false)) return null;
        if (_checkMatchesOBOIdentity && checkMatchesOBOIdentity()) return null;
        if (_checkIsAdmin && checkIsAdmin()) return null;
        if (_checkIsOBOAdmin && checkIsOBOAdmin()) return null;
        
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
    /** Run the enabled prevention routines.  A failure of any one of them causes
     * the request to be aborted.
     * 
     * @return null if there are no problems, otherwise return an error message.
     */
    public String preventMsg()
    {
    	// All enabled prevention routines must succeed for overall success. 
    	if (_preventAdminRole) {
    		String errorMsg = preventAdminRole();
    		if (errorMsg != null) return errorMsg;
    	}
    	
    	// No problems found.
    	return null;
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* validateRequestTenantContext:                                                */
    /* ---------------------------------------------------------------------------- */
    /** This method first validates the thread context, which allows all subsequent 
     * checks to be performed without risk of NPEs on threadlocal data.  
     * 
     * For service tokens, we check that the jwt tenant is allowed to be used on 
     * behalf of the request tenant.  Also check that the request tenant is the
     * same as the OBO tenant.
     * 
     * For user tokens, we check that the jwt and request tenants are the same.
     * 
     * This method does not use the _reqUser field, so it's ok if it's null.
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
        
        // Make sure the request tenant has been assigned.  We're not so strict
        // with the user, which is allowed to be null and is checked at use sites.
        if (StringUtils.isBlank(_reqTenant)) {
            var msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTenantContext", "_reqTenant");
            _log.error(msg);
            return msg;
        }
        
        // Collect some context information.
        String jwtTenant = _threadContext.getJwtTenantId();
        AccountType accountType = _threadContext.getAccountType();
        
        // Service accounts are allowed more latitude than user accounts.
        // Specifically, they can specify tenants other than that in their jwt.
        if (accountType == AccountType.service) {
            // Make sure the OBO tenant is the same as the request tenant.
            var oboTenant = _threadContext.getOboTenantId();
            if (!_reqTenant.equals(oboTenant)) {
                String jwtUser = _threadContext.getJwtUser();
                String msg = MsgUtils.getMsg("SK_REQUEST_OBO_TENANT_MISMATCH", 
                                             jwtUser, jwtTenant, oboTenant, _reqTenant);
                _log.error(msg);
                return msg;
            }
            
            // Make sure the service's jwt covers the request tenant.
            boolean allowedTenant;
            try {allowedTenant = TapisRestUtils.isAllowedTenant(jwtTenant, _reqTenant);}
                catch (Exception e) {
                    String jwtUser = _threadContext.getJwtUser();
                    var msg = MsgUtils.getMsg("TAPIS_SECURITY_ALLOWABLE_TENANT_ERROR", 
                                              jwtUser, jwtTenant, _reqTenant);
                    _log.error(msg, e);
                    return msg;
                }
            
            // Can the new tenant id be used by the jwt tenant?
            if (!allowedTenant) {
                String jwtUser = _threadContext.getJwtUser();
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_TENANT_NOT_ALLOWED", 
                                             jwtUser, jwtTenant, _reqTenant);
                _log.error(msg);
                return msg;
            }
        } else {
            // User tokens require exact tenant matches.
            if (!jwtTenant.equals(_reqTenant)) {
            	String jwtUser = _threadContext.getJwtUser();
                var msg = MsgUtils.getMsg("SK_UNEXPECTED_TENANT_VALUE", jwtUser,
                                          jwtTenant, _reqTenant, accountType.name());
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
    private boolean checkMatchesJwtIdentity(boolean failProvisionally)
    {
        // Make sure the request user has been assigned for this check.
        if (_reqUser == null) {
            var msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "checkMatchesJwtIdentity", "_reqUser");
            _log.error(msg);
            if (failProvisionally) _provisionalFailedChecks.add("MatchesJwtIdentity");
              else _failedChecks.add("MatchesJwtIdentity");
            return false;
        }
            
        // See if the jwt and request user@tenant are the same.
        if (_threadContext.getJwtTenantId().equals(_reqTenant) &&
            _threadContext.getJwtUser().equals(_reqUser)) return true;
        
        // Record failure.
        if (failProvisionally) _provisionalFailedChecks.add("MatchesJwtIdentity");
          else _failedChecks.add("MatchesJwtIdentity");
        return false;
    }

    /* ---------------------------------------------------------------------------- */
    /* checkMatchesOBOIdentity:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Check that the identity provided on the OBO headers is one on behalf of whom
     * this service is acting.  First, the service's tenant as expressed in the jwt 
     * must be allowed to act on behalf of the request tenant. This has already 
     * been checked in validateTenantContext().  Next, the request user and tenant 
     * must match the OBO user and tenant as originally specified in the request
     * headers.  This effectively authorizes the service to perform any action on 
     * behalf of the OBO identity.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkMatchesOBOIdentity()
    {
        // Make sure the request user has been assigned for this check.
        if (_reqUser == null) {
            var msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "checkServiceOBO", "_reqUser");
            _log.error(msg);
            _failedChecks.add("MatchesOnBehalfOfIdentity");
            return false;
        }
        
        // Start pessimistically.
        boolean allowedIdentity = false;
        
        // Only services need apply.
        if (_threadContext.getAccountType() == AccountType.service) {
            // Restrict the request identity to be the OBO identity.
            if (_reqTenant.equals(_threadContext.getOboTenantId()) &&
                _reqUser.equals(_threadContext.getOboUser())) 
               allowedIdentity = true;
        }
        
        // What happened?
        if (allowedIdentity) return true;
        _failedChecks.add("MatchesOnBehalfOfIdentity");
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
    /* checkIsOBOAdmin:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Check that the on-behalf-of identity is an administrator.
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkIsOBOAdmin()
    {
        // See if the obo user has admin privileges.
        boolean authorized = false;
        if (_threadContext.getAccountType() == AccountType.service)
            try {
                var userImpl = UserImpl.getInstance();
                authorized = userImpl.hasRole(_threadContext.getOboTenantId(),
                                              _threadContext.getOboUser(), 
                                              new String[] {UserImpl.ADMIN_ROLE_NAME}, 
                                              AuthOperation.ANY);
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                             _threadContext.getOboTenantId(), 
                                             _threadContext.getOboUser(), e.getMessage());
                _log.error(msg, e);
            }
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("IsOboAdmin");
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
    /* checkOwnedRoles:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Checks the user identified in the request is the role owner.  If more than one
     * role is in the _ownedRoles list, all roles must be owned by the request user. 
     * 
     * @return true if passes check, false otherwise.
     */
    private boolean checkOwnedRoles()
    {
        // Idiot check prohibits no roles from implying authorization. 
        if (_ownedRoles == null || _ownedRoles.isEmpty()) return false;
        
        // Make sure the request user has been assigned for this check.
        if (_reqUser == null) {
            var msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "checkOwnedRoles", "_reqUser");
            _log.error(msg);
            _failedChecks.add("OwnedRoles");
            return false;
        }
        
        // Start optimistically.
        boolean authorized = true;
        try {
        	// This block checks the role owner identity, which add a 4th identity
        	// to the usual 3: jwt, req, obo.
            var roleImpl = RoleImpl.getInstance();
            for (String roleName : _ownedRoles) {
                // The request and role tenants are guaranteed to be the same
            	// because we use the request tenant in the retrieval.
                var skRole = roleImpl.getRoleByName(_reqTenant, roleName);
                
                // Bad news.
                if (skRole == null) {authorized = false; break;}
                
                // User jwt case.  Make sure the user identified in the JWT
                // is owner of the role.
                if (_threadContext.getAccountType() == AccountType.user) {
                	if (!_threadContext.getJwtUser().equals(skRole.getOwner())) {
                		authorized = false; break;
                	}
                } 
                
                // Service jwt case. Either one of two conditions must be true
                // for the check to pass.
                //
                // Condition 1 checks that the user on behalf of whom this
                // request is being made (the obo user) is the owner of the role.
                // We already know the request and obo tenants are the same, so
                // by transitivity we know the obo tenant and role tenant are the
                // same.
                //
                // Condition 2 checks that the jwt user@tenant matches the 
                // role's user@tenant.
                //
                // If neither of these conditions holds then the request will be
                // rejected as unauthorized.
                if (_threadContext.getAccountType() == AccountType.service) {
                	if (!(_threadContext.getOboUser().equals(skRole.getOwner()))
                		  &&
                		!(_threadContext.getJwtUser().equals(skRole.getOwner()) &&
                          _threadContext.getJwtTenantId().equals(skRole.getTenant()))
                	     )		
                	{
                		authorized = false; break;
                	}
                }
            }
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_USER_GET_ROLE_NAMES_ERROR", 
                                         _threadContext.getJwtTenantId(), 
                                         _threadContext.getJwtUser(), e.getMessage());
            _log.error(msg, e);
            authorized = false;
        }
        
        // We recognize that any service could grant a role to any user by simply
        // performing the following:
        //
        //  1. Querying the role to learn its owner.
        //  2. Set the obo user to equal the role owner.
        //  3. Set the request user@tenant to be any user, including the service itself.
        //  4. Issue the grantRole call.
        //
        // A rogue or faulty service could grant any role to any user in a
        // tenant, including the tenant admin role.  The calling method should
        // prevent the granting of the tenant admin role.
        
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
                authorized = secretCheckIsTokensService(true);
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
            _threadContext.getJwtUser().equals(SYSTEMS_SERVICE_NAME)) 
           authorized = true;
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("IsSystemsService");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* secretCheckIsTokensService:                                                  */
    /* ---------------------------------------------------------------------------- */
    private boolean secretCheckIsTokensService(boolean failProvisionally)
    {
        // Start pessimistically.
        boolean authorized = false;
        
        // Are the path parms configured and is the caller the tokens service?  
        if (_threadContext.getAccountType() == AccountType.service &&
            _threadContext.getJwtUser().equals(TOKENS_SERVICE_NAME)) 
           return true;
        
        // Failure.
        if (failProvisionally) _provisionalFailedChecks.add("IsTokensService");
          else _failedChecks.add("IsTokensService");
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
    	// We provisionally fail the first check.
        boolean authorized = secretCheckIsTokensService(false) ||
                             secretCheckServiceRequestIdentity("ValidatePassword");
        if (!authorized) reportProvisionalFailures();
        
        return authorized;
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
            _threadContext.getJwtTenantId().equals(_reqTenant) &&
            _threadContext.getJwtUser().equals(_reqUser))
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
            _threadContext.getJwtTenantId().equals(_reqTenant) &&
            _threadContext.getJwtUser().equals(_reqUser))
            authorized = true;
        
        // What happened?
        if (authorized) return true;
        _failedChecks.add("UserSecret");
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* reportProvisionalFailures:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Use this method to report failed checks that could have been ignored if a 
     * later check passed, but that didn't happen.  This approach allows us to detect
     * failures we checking for a non-empty _failedChecks list.
     */
    private void reportProvisionalFailures() {_failedChecks.addAll(_provisionalFailedChecks);}
    
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
            		               _threadContext.getJwtTenantId(), 
            		               _threadContext.getJwtUser());
        }
        
    	// No problem.
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
                                     _threadContext.getJwtTenantId(),
                                     _threadContext.getJwtUser(),
                                     _threadContext.getOboTenantId(),
                                     _threadContext.getOboUser(),
                                     _threadContext.getAccountType(),
                                     s);
        _log.error(msg);
        return msg;
    }
}

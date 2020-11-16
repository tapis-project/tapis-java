package edu.utexas.tacc.tapis.security.api.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.security.config.RuntimeParameters;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

/** This class makes sure each tenant has an administrator role defined and
 * at least one user assigned that role.  It gets its tenant information from
 * the map returned by the tenants service.
 * 
 * @author rcardone
 */
public final class TenantInit 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(TenantInit.class);
    
    // The user representing SK.
    private static final String SK_USER = UserImpl.SK_USER;

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Non-null tenant map of tenant ids to tenant objects.
    private final Map<String,Tenant> _tenantMap;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private TenantInit(Map<String,Tenant> tenantMap)
    {
    	// Map of tenant ids to tenant objects.
        _tenantMap = tenantMap;
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initializeTenants:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Initialize the roles for all known tenants.
     * 
     * @param tenantMap map of tenant ids to tenant objects
     */
    public static void initializeTenants(Map<String,Tenant> tenantMap)
    {
        // Maybe there's nothing to do.
        if (tenantMap == null || tenantMap.isEmpty()) return;
        
        // Invoke the initialize method on a new object of this type.
        new TenantInit(tenantMap).initialize();
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initialize:                                                            */
    /* ---------------------------------------------------------------------- */
    private void initialize()
    {
        // Get the site-master tenant id.
        final String site = RuntimeParameters.getInstance().getSiteId();
        final String siteMasterTenant = TenantManager.getInstance().getSiteMasterTenantId(site);
        
        // One time initialization for tenants service.
        initializeTenantServiceRole(siteMasterTenant);
        
        // Inspect each tenant.
        for (var entry : _tenantMap.entrySet()) 
        {
        	// The tenant id is the key.
        	String tenantId = entry.getKey();
        	Tenant tenant   = entry.getValue();
        	
        	// Guarantee that there's at least one administrator id in each tenant.
        	// Administrators are users assigned the $!tenant_admin role. 
        	initializeTenantAdmin(tenantId, siteMasterTenant, tenant.getAdminUser());
        	
        	// Assign authenticator roles to services, which allows those services
        	// to request user tokens from the Tokens service.  The roles conform
        	// to the format <tenant>_token_generator.
        	initializeAuthenticators(tenantId, siteMasterTenant, tenant.getTokenGenServices());
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* initializeTenantServiceRole:                                           */
    /* ---------------------------------------------------------------------- */
    private void initializeTenantServiceRole(String siteMasterTenant)
    {
        // Designate the tenants service identifiers.
        final String primaryTenant = TapisConstants.PRIMARY_SITE_TENANT;
        final String tenantService = TapisConstants.SERVICE_NAME_TENANTS;
        final String roleName = UserImpl.TENANT_CREATOR_ROLE;
        
        // Get the list of all users with the tenant creator role.
        List<String> creators = null;
        try {creators = UserImpl.getInstance().getUsersWithRole(primaryTenant, roleName);}
        catch (TapisNotFoundException e) {
            String msg = MsgUtils.getMsg("SK_TENANT_INIT_WARN", primaryTenant, 
                                          roleName, e.getMessage());
            _log.warn(msg);
        }
        catch (Exception e) {
            // This should not happen even if the tenant and role don't exist.
            // We log the problem but proceed.
            String msg = MsgUtils.getMsg("SK_GET_USERS_WITH_ROLE_ERROR", primaryTenant, 
                                        roleName, e.getMessage());
            _log.error(msg, e);
        } 
        
        // Does the tenants service have the required role?
        if (creators != null && creators.contains(tenantService)) return;
        
        // ------------------ Tenant Creator Role -------------------
        // Create and assign the tenant creator role to the Tapis tenants service.
        try {
            // Assign role to the default authenticator for this tenant, creating
            // the role if necessary.  This calls the internal grant method 
            // that does not check whether the requestor is an administrator.
            String desc = "Tenants service creator role";
            UserImpl.getInstance().grantRoleInternal(roleName, primaryTenant, desc, 
            		                                 tenantService, primaryTenant,
            		                                 SK_USER, siteMasterTenant);
            String msg = MsgUtils.getMsg("SK_TENANT_CREATOR_ASSIGNED", primaryTenant, tenantService, roleName);
            _log.info(msg);
        } catch (Exception e) {
            // Log the error and continue on.
            String msg = MsgUtils.getMsg("SK_TENANT_INIT_CREATOR_ERROR", primaryTenant, 
                                         tenantService, e.getMessage());
            _log.error(msg, e);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* initializeTenantAdmin:                                                 */
    /* ---------------------------------------------------------------------- */
    private void initializeTenantAdmin(String tenant, String siteMasterTenant, 
    		                           String adminUser)
    {
        // Get the list of admins in the tenant.
        List<String> admins = null;
        try {
            admins = UserImpl.getInstance().getUsersWithRole(tenant, 
                                                UserImpl.ADMIN_ROLE_NAME);
        }
        catch (TapisNotFoundException e) {
            String msg = MsgUtils.getMsg("SK_TENANT_INIT_WARN", tenant, 
                                          UserImpl.ADMIN_ROLE_NAME, e.getMessage());
            _log.warn(msg);
        }
        catch (Exception e) {
            // This should not happen even if the tenant and role don't exist.
            // We log the problem but proceed.
            String msg = MsgUtils.getMsg("SK_GET_USERS_WITH_ROLE_ERROR", tenant, 
                                         UserImpl.ADMIN_ROLE_NAME, e.getMessage());
            _log.error(msg, e);
        } 
        
        // Did we get at least one admin?
        if (admins != null && !admins.isEmpty()) return;
        
        // ----------------------- Admin Role -----------------------
        // Create and assign the admin role to the default tenant administrator.
        try {
            // Assign role to the default administrator for this tenant, creating
            // the role if necessary.  This calls the internal grant method 
            // that does not check whether the requestor is an administrator. 
        	UserImpl.getInstance().grantAdminRoleInternal(adminUser, tenant, SK_USER, siteMasterTenant);
        	String msg = MsgUtils.getMsg("SK_TENANT_ADMIN_ASSIGNED", tenant, adminUser,
                                  		 UserImpl.ADMIN_ROLE_NAME);
            _log.info(msg);
        } 
        catch (Exception e) {
            // Log the error and continue on.
            String msg = MsgUtils.getMsg("SK_TENANT_INIT_ADMIN_ERROR", tenant, 
                                         adminUser, e.getMessage());
            _log.error(msg, e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* initializeAuthenticators:                                              */
    /* ---------------------------------------------------------------------- */
    private void initializeAuthenticators(String tenant, String siteMasterTenant,
    		                              List<String> tokgenServices)
    {
        // The role is always owned by tokens@<site-master>, always defined in the
        // site-master tenant, and always assigned to services in the site-master tenant.
        final String tokgenRoleTenant = siteMasterTenant;
        final String tokgenOwner = "tokens";
        final String tokgenOwnerTenant = siteMasterTenant;
        final String roleName = UserImpl.getInstance().makeTenantTokenGeneratorRolename(tenant);
        final String desc = "Tenant token generator role";
        
        // Create and assign the authenticator role to the tenant's auth service.
        try {
            // Assign role to the default authenticator for this tenant, creating
            // the role if necessary.  This calls the internal grant method 
            // that does not check whether the requestor is an administrator.
            //
            // Assign each service.
            for (String tokgenService : tokgenServices) { 
            	UserImpl.getInstance().grantRoleInternal(roleName, tokgenRoleTenant, desc,
            			                                 tokgenService, tokgenRoleTenant,
            			                                 tokgenOwner, tokgenOwnerTenant);
            	String msg = MsgUtils.getMsg("SK_TENANT_TOKEN_GEN_ASSIGNED", tokgenRoleTenant,
                                         	 tokgenService, roleName);
            	_log.info(msg);
            }
        } catch (Exception e) {
            // Log the error and continue on.
        	String s = tokgenServices.stream().collect(Collectors.joining(", "));
            String msg = MsgUtils.getMsg("SK_TENANT_INIT_TOKGEN_ERROR", tokgenRoleTenant, 
                                         s, roleName, e.getMessage());
            _log.error(msg, e);
        }
    }
}

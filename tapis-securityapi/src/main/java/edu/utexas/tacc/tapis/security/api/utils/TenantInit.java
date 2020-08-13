package edu.utexas.tacc.tapis.security.api.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
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
    // Non-null tenant map.
    private final Map<String,Tenant> _tenantMap;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private TenantInit(Map<String,Tenant> tenantMap)
    {
        _tenantMap = tenantMap;
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initializeTenants:                                                     */
    /* ---------------------------------------------------------------------- */
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
        // One time initialization for tenants service.
        initializeTenantServiceRole();
        
        // Inspect each tenant.
        for (var entry : _tenantMap.entrySet()) 
        {
            // Current tenant.
            String tenant = entry.getKey();
            
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
            if (admins != null && !admins.isEmpty()) continue;
            
            // ----------------------- Admin Role -----------------------
            // TODO: **** get user from tenant record and get site master from sites table
            final String adminUser = "admin";         // ************* Temp code
            final String siteMasterTenant = "master"; // ************* Temp code
            
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
            
            // ------------------- Authenticator Role -------------------
            // TODO: **** get authenticator service from tenant record
            final String[] tokgenServices = {"authenticator", "abaco"};  // ************* Temp code
            
            // TODO: **** get site-master from from sites table
            // The role is always owned by tokens@<site-master>, always defined in the
            // site-master tenant, and always assigned to services in the site-master tenant.
            final String tokgenRoleTenant = "master";     // ************* Temp code
            final String tokgenOwner = "tokens";
            final String tokgenOwnerTenant = "master";    // ************* Temp code
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
            	var list = Arrays.asList(tokgenServices);
            	String s = list.stream().collect(Collectors.joining(", "));
                String msg = MsgUtils.getMsg("SK_TENANT_INIT_TOKGEN_ERROR", tokgenRoleTenant, 
                                             s, roleName, e.getMessage());
                _log.error(msg, e);
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* initializeTenantServiceRole:                                           */
    /* ---------------------------------------------------------------------- */
    private void initializeTenantServiceRole()
    {
        // Designate the tenants service identifiers.
        final String primaryTenant = TapisConstants.PRIMARY_SITE_TENANT;
        final String tenantService = TapisConstants.SERVICE_NAME_TENANTS;
        final String roleName = UserImpl.TENANT_CREATOR_ROLE;
        final String siteMasterTenant = "master"; // TODO: ************* Temp code
        
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
}

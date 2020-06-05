package edu.utexas.tacc.tapis.security.api.utils;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
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
                String msg = MsgUtils.getMsg("SK_TENANT_INIT_ADMIN_WARN", tenant, 
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
            
            // TODO: get user from tenant record
            String user = "admin";  // ************* Temp code
            
            // Create and assign the admin role to the default tenant administrator.
            try {
                // Assign role to the default administrator for this tenant, creating
                // the role if necessary.  This calls the internal grant method 
                // that does not check whether the requestor is an administrator. 
                UserImpl.getInstance().grantAdminRoleInternal(tenant, SK_USER, user);
                String msg = MsgUtils.getMsg("SK_TENANT_ADMIN_ASSIGNED", tenant, user,
                                             UserImpl.ADMIN_ROLE_NAME);
                _log.info(msg);
            } catch (Exception e) {
                // Log the error and continue on.
                String msg = MsgUtils.getMsg("SK_TENANT_INIT_ERROR", tenant, 
                                             user, e.getMessage());
                _log.error(msg, e);
            }
        }
    }
}

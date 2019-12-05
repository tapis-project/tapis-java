package edu.utexas.tacc.tapis.security.authz.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** A minimal permission class that represents the database records without
 * date and user fields.
 * 
 * @author rcardone
 */
public final class SkRolePermissionShort 
{
    // Fields.
    private int     id;
    private String  tenant;
    private int     roleId;
    private String  permission;
    
    // Constructor.
    public SkRolePermissionShort(int id, String tenant, int roleId, String permission)
    {
        this.id = id;
        this.tenant = tenant;
        this.roleId = roleId;
        this.permission = permission;
    }
    
    // Constructor.
    public SkRolePermissionShort(){}
    
    @Override
    public String toString() {return TapisUtils.toString(this);}

    // Accessors.
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getTenant() {
        return tenant;
    }
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
    public int getRoleId() {
        return roleId;
    }
    public void setRoleId(int roleId) {
        this.roleId = roleId;
    }
    public String getPermission() {
        return permission;
    }
    public void setPermission(String permission) {
        this.permission = permission;
    }
}

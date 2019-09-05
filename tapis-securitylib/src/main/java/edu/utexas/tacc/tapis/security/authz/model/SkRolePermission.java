package edu.utexas.tacc.tapis.security.authz.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class SkRolePermission
{
    private int     id;
    private String  tenant;
    private int     roleId;
    private int     permissionId;
    private Instant created;
    private String  createdby;
    private Instant updated;
    private String  updatedby;

    @Override
    public String toString() {return TapisUtils.toString(this);}

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
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

    public int getPermissionId() {
        return permissionId;
    }

    public void setPermissionId(int permissionId) {
        this.permissionId = permissionId;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public String getCreatedby() {
        return createdby;
    }

    public void setCreatedby(String createdby) {
        this.createdby = createdby;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    public String getUpdatedby() {
        return updatedby;
    }

    public void setUpdatedby(String updatedby) {
        this.updatedby = updatedby;
    }
}

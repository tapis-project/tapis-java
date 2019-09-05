package edu.utexas.tacc.tapis.security.authz.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class SkRoleTree
{
    private int     id;
    private String  tenant;
    private int     parentRoleId;
    private int     childRoleId;
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

    public int getParentRoleId() {
        return parentRoleId;
    }

    public void setParentRoleId(int parentRoleId) {
        this.parentRoleId = parentRoleId;
    }

    public int getChildRoleId() {
        return childRoleId;
    }

    public void setChildRoleId(int childRoleId) {
        this.childRoleId = childRoleId;
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

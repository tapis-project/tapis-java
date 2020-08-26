package edu.utexas.tacc.tapis.security.authz.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class SkUserRole
{
    private int     id;
    private String  tenant;
    private String  userName;
    private int     roleId;
    private Instant created;
    private String  createdby;
    private String  createdbyTenant;
    private Instant updated;
    private String  updatedby;
    private String  updatedbyTenant;

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

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getRoleId() {
        return roleId;
    }

    public void setRoleId(int roleId) {
        this.roleId = roleId;
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

	public String getCreatedbyTenant() {
		return createdbyTenant;
	}

	public void setCreatedbyTenant(String createdbyTenant) {
		this.createdbyTenant = createdbyTenant;
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

	public String getUpdatedbyTenant() {
		return updatedbyTenant;
	}

	public void setUpdatedbyTenant(String updatedbyTenant) {
		this.updatedbyTenant = updatedbyTenant;
	}
}

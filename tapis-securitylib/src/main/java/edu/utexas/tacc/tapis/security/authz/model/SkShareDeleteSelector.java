package edu.utexas.tacc.tapis.security.authz.model;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SkShareDeleteSelector 
{
    private String  tenant;
    private String  grantor;
    private String  grantee;         
    private String  resourceType;    
    private String  resourceId1;     
    private String  resourceId2;     
    private String  privilege;
    private String  createdBy;
    private String  createdByTenant;
    
    // Validate
    public void validate() throws TapisException
    {
        // Exceptions can be throw from here.
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "tenant");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(grantor)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "grantor");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(grantee)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "grantee");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(resourceType)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "resourceType");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(resourceId1)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "resourceId1");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(privilege)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "privilege");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(createdBy)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "createdBy");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(createdByTenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "deleteShare", "createdByTenant");
            throw new TapisException(msg);
        }
    }
    
    // Accessors.
    public String getTenant() {
        return tenant;
    }
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
    public String getGrantor() {
        return grantor;
    }
    public void setGrantor(String grantor) {
        this.grantor = grantor;
    }
    public String getGrantee() {
        return grantee;
    }
    public void setGrantee(String grantee) {
        this.grantee = grantee;
    }
    public String getResourceType() {
        return resourceType;
    }
    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }
    public String getResourceId1() {
        return resourceId1;
    }
    public void setResourceId1(String resourceId1) {
        this.resourceId1 = resourceId1;
    }
    public String getResourceId2() {
        return resourceId2;
    }
    public void setResourceId2(String resourceId2) {
        this.resourceId2 = resourceId2;
    }
    public String getPrivilege() {
        return privilege;
    }
    public void setPrivilege(String privilege) {
        this.privilege = privilege;
    }
    public String getCreatedBy() {
        return createdBy;
    }
    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }
    public String getCreatedByTenant() {
        return createdByTenant;
    }
    public void setCreatedByTenant(String createdByTenant) {
        this.createdByTenant = createdByTenant;
    }
}

package edu.utexas.tacc.tapis.security.authz.model;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SkSharePrivilegeSelector 
{
    private String  tenant;
    private String  grantee;         
    private String  resourceType;    
    private String  resourceId1;     
    private String  resourceId2;     
    private String  privilege;    
    private boolean excludePublic;
    private boolean excludePublicNoAuthn;
    
    public void validate() throws TapisException
    {
        // Exceptions can be throw from here.
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasPrivilege", "tenant");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(grantee)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasPrivilege", "grantee");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(resourceType)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasPrivilege", "resourceType");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(resourceId1)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasPrivilege", "resourceId1");
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(privilege)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasPrivilege", "privilege");
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
    public boolean isExcludePublic() {
        return excludePublic;
    }
    public void setExcludePublic(boolean excludePublic) {
        this.excludePublic = excludePublic;
    }
    public boolean isExcludePublicNoAuthn() {
        return excludePublicNoAuthn;
    }
    public void setExcludePublicNoAuthn(boolean excludePublicNoAuthn) {
        this.excludePublicNoAuthn = excludePublicNoAuthn;
    }
}

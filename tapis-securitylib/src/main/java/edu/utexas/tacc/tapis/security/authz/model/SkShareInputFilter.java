package edu.utexas.tacc.tapis.security.authz.model;

public class SkShareInputFilter 
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
    private int     id; // actual share id's start at 1
    private boolean includePublicGrantees = true;
    private boolean requireNullId2 = true;
    
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
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public boolean isIncludePublicGrantees() {
        return includePublicGrantees;
    }
    public void setIncludePublicGrantees(boolean includePublicGrantees) {
        this.includePublicGrantees = includePublicGrantees;
    }
    public boolean isRequireNullId2() {
        return requireNullId2;
    }
    public void setRequireNullId2(boolean requireNullId2) {
        this.requireNullId2 = requireNullId2;
    }
}

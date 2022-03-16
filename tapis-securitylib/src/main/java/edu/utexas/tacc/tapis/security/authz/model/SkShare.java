package edu.utexas.tacc.tapis.security.authz.model;

import java.time.Instant;

public final class SkShare 
{
    private int      id;
    private String   tenant;
    private String   grantor;
    private String   grantee;
    private String   resourceType;
    private String   resourceId1;
    private String   resourceId2;
    private String   privilege;
    private Instant  created;
    private String   createdBy;
    private String   createdByTenant;
    
    // Get a descriptive string representation of the resource ids.
    public String printResource() {
        return resourceId2 == null ? resourceId1 : resourceId1 + ":" + resourceId2;
    }
    
    public String dumpContent() {
        return "id=" + id + ", " +
               "tenant=" + tenant + ", " +
               "grantor=" + grantor + ", " +
               "grantee=" + grantee + ", " +
               "resourceType=" + resourceType + ", " +
               "resourceId=" + printResource() + ", " +
               "privilege=" + privilege + ", " +
               "created=" + created.toString() + ", " +
               "createdBy=" + createdBy + ", " +
               "createdByTenant=" + createdByTenant;
    }
    
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
    public Instant getCreated() {
        return created;
    }
    public void setCreated(Instant created) {
        this.created = created;
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

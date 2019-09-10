package edu.utexas.tacc.tapis.security.authz.model;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRolePermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleTreeDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkUserRoleDao;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class SkRole
{
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkRole.class);
    
    private int     id;
    private String  tenant;
    private String  name;
    private String  description;
    private Instant created;
    private String  createdby;
    private Instant updated;
    private String  updatedby;

    @Override
    public String toString() {return TapisUtils.toString(this);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addChildRole:                                                          */
    /* ---------------------------------------------------------------------- */
    public void addChildRole(String user, String childRoleName) throws TapisException
    {
        // We expect roles to have been populated from a database record,
        // but nothing stops someone from constructing a homemade object and
        // attempting to put cross-tenant junk into the database. The dao method 
        // guards against that possibility.
        try {
            SkRoleTreeDao dao = new SkRoleTreeDao();
            dao.assignChildRole(tenant, user, id, childRoleName);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* addPermission:                                                         */
    /* ---------------------------------------------------------------------- */
    public void addPermission(String user, String permissionName) throws TapisException
    {
        // We expect roles to have been populated from a database record,
        // but nothing stops someone from constructing a homemade object and
        // attempting to put cross-tenant junk into the database. The dao method 
        // guards against that possibility.
        try {
            SkRolePermissionDao dao = new SkRolePermissionDao();
            dao.assignPermission(tenant, user, id, permissionName);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* addUser:                                                               */
    /* ---------------------------------------------------------------------- */
    public void addUser(String assigner, String assignee) throws TapisException
    {
        // We expect roles to have been populated from a database record,
        // but nothing stops someone from constructing a homemade object and
        // attempting to put cross-tenant junk into the database. The dao method 
        // guards against that possibility.
        try {
            SkUserRoleDao dao = new SkUserRoleDao();
            dao.assignRole(tenant, assigner, assignee, id);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDescendantRoleNames:                                                */
    /* ---------------------------------------------------------------------- */
    /** Get this list of names of the children roles of this role. 
     * 
     * @return list of children role names
     * @throws TapisException
     */
    public List<String> getDescendantRoleNames() throws TapisException
    {
        List<String> list = null;
        try {
            SkRoleDao dao = new SkRoleDao();
            list = dao.getDescendantRoleNames(id);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        return list;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getAncestorRoleNames:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Get this list of names of the ancestor roles of this role. 
     * 
     * @return list of ancestor role names
     * @throws TapisException
     */
    public List<String> getAncestorRoleNames() throws TapisException
    {
        List<String> list = null;
        try {
            SkRoleDao dao = new SkRoleDao();
            list = dao.getAncestorRoleNames(id);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        return list;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getTransitivePermissionNames:                                          */
    /* ---------------------------------------------------------------------- */
    /** Get this list of permission names assigned to this role and all of its
     * descendants. 
     * 
     * @return list of permissions names associated with this role transitively
     * @throws TapisException
     */
    public List<String> getTransitivePermissionNames() throws TapisException
    {
        List<String> list = null;
        try {
            SkRoleDao dao = new SkRoleDao();
            list = dao.getTransitivePermissionNames(id);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        return list;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getTransitivePermissions:                                              */
    /* ---------------------------------------------------------------------- */
    /** Get this list of permission values (i.e. constraints) assigned to this 
     * role and all of its descendants. 
     * 
     * @return list of permissions associated with this role transitively
     * @throws TapisException
     */
    public List<String> getTransitivePermissions() throws TapisException
    {
        List<String> list = null;
        try {
            SkRoleDao dao = new SkRoleDao();
            list = dao.getTransitivePermissions(id);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        return list;
    }
    
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

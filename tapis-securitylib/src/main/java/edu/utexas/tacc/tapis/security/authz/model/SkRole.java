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
import io.swagger.v3.oas.annotations.media.Schema;

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
    /** Add a child role to this role.  Return 0 if the child role was already 
     * assigned to this role, 1 if this is a new child assignment to this role.
     * 
     * @param user
     * @param childRoleName
     * @return
     * @throws TapisException
     */
    public int addChildRole(String user, String childRoleName) throws TapisException
    {
        // We expect roles to have been populated from a database record,
        // but nothing stops someone from constructing a homemade object and
        // attempting to put cross-tenant junk into the database. The dao method 
        // guards against that possibility.
        int rows;
        try {
            SkRoleTreeDao dao = new SkRoleTreeDao();
            rows = dao.assignChildRole(tenant, user, name, childRoleName);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* addPermission:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Assign a permission to this role.  Return 0 if the permission was already 
     * assigned to the role, 1 if this is a new permission assignment to the role.
     * 
     * @param user the user assigning the permission
     * @param permission the permission specification being assigned
     * @return the number of rows affected (0 or 1)
     * @throws TapisException
     */
    public int addPermission(String user, String permission) throws TapisException
    {
        // We expect roles to have been populated from a database record,
        // but nothing stops someone from constructing a homemade object and
        // attempting to put cross-tenant junk into the database. The dao method 
        // guards against that possibility.
        int rows;
        try {
            SkRolePermissionDao dao = new SkRolePermissionDao();
            rows = dao.assignPermission(tenant, user, id, permission);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* addUser:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Add this role to a user.  Return 0 if the user was already assigned the
     * role, 1 if this is a new role assignment to the user.
     * 
     * @param assigner the user assigning the role
     * @param assignee the user being assigned the role
     * @return the number of rows affected (0 or 1)
     * @throws TapisException
     */
    public int addUser(String assigner, String assignee) throws TapisException
    {
        // We expect roles to have been populated from a database record,
        // but nothing stops someone from constructing a homemade object and
        // attempting to put cross-tenant junk into the database. The dao method 
        // guards against that possibility.
        int rows;
        try {
            SkUserRoleDao dao = new SkUserRoleDao();
            rows = dao.assignUserRole(tenant, assigner, assignee, id);
        } catch (Exception e) {
            _log.error(e.getMessage()); // details already logged
            throw e;
        }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDescendantRoleNames:                                                */
    /* ---------------------------------------------------------------------- */
    /** Get this list of names of the children roles of this role. 
     * 
     * @return list of children role names
     * @throws TapisException
     */
    @Schema(hidden = true)
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
    @Schema(hidden = true)
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
    /* getTransitivePermissions:                                              */
    /* ---------------------------------------------------------------------- */
    /** Get this list of permission values (i.e. constraint strings) assigned 
     * to this role and all of its descendants. The permission values are 
     * returned in alphabetic order. 
     * 
     * @return non-null, ordered list of permissions associated with this role 
     *         directly and transitively
     * @throws TapisException
     */
    @Schema(hidden = true)
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
    
    /* ---------------------------------------------------------------------- */
    /* getImmediatePermissions:                                               */
    /* ---------------------------------------------------------------------- */
    /** Get this list of permission values (i.e. constraint strings) directly 
     * assigned to this role. The permission values are returned in alphabetic 
     * order. 
     * 
     * @return non-null, ordered list of permissions associated with this role 
     *         directly 
     * @throws TapisException
     */
    @Schema(hidden = true)
    public List<String> getImmediatePermissions() throws TapisException
    {
        List<String> list = null;
        try {
            SkRoleDao dao = new SkRoleDao();
            list = dao.getImmediatePermissions(tenant, id);
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

    @Schema(type = "string")
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

    @Schema(type = "string")
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

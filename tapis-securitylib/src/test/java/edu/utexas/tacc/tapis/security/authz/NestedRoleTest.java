package edu.utexas.tacc.tapis.security.authz;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.security.authz.dao.SkPermissionDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkUserRoleDao;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class NestedRoleTest 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // The tenant and user referenced in this test.
    private static final String tenant = "testtenant";
    private static final String user   = "testuser";
    private static final String user1  = "testuser1";
    private static final String user2  = "testuser2";
    private static final String user3  = "testuser3";
    private static final String user4  = "testuser4";
    
    /* ********************************************************************** */
    /*                            Main Test Method                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* testAll:                                                               */
    /* ---------------------------------------------------------------------- */
    @Test
    public void testAll() throws TapisException
    {
        // Delete permissions created by prior runs of this test.
        deletePermissions();
        
        // Delete roles created by prior runs of this test.
        deleteRoles();
        
        // Create new roles.
        createRoles();
        
        // Assign children roles to their parents.
        assignChildrenRoles();
        
        // Query role hierarchy top down.
        checkRoleDescendants();
        
        // Query role hierarchy bottom up.
        checkRoleAncestors();
        
        // Create new permissions.
        createPermissions();
        
        // Assign permissions to roles.
        assignRolePermissions();
        
        // Query role permissions.
        checkRolePermissions();
        
        // Assign roles to users.
        assignUserRoles();
        
        // Query user roles.
        checkUserRoles();
        
        // Query user permissions.
        checkUserPermissions();
        
        // Delete permissions created by prior runs of this test.
        deletePermissions();
        
        // Delete roles created by prior runs of this test.
        deleteRoles();
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* deletePermissions:                                                     */
    /* ---------------------------------------------------------------------- */
    private void deletePermissions() throws TapisException
    {
        SkPermissionDao dao = new SkPermissionDao();
        dao.deletePermission(tenant,"NestedTestPerm1");
        dao.deletePermission(tenant,"NestedTestPerm2");
        dao.deletePermission(tenant,"NestedTestPerm3");
        dao.deletePermission(tenant,"NestedTestPerm4");
    }
    
    /* ---------------------------------------------------------------------- */
    /* deleteRoles:                                                           */
    /* ---------------------------------------------------------------------- */
    private void deleteRoles() throws TapisException
    {
        SkRoleDao dao = new SkRoleDao();
        dao.deleteRole(tenant, "NestedTestRole1");
        dao.deleteRole(tenant, "NestedTestRole2");
        dao.deleteRole(tenant, "NestedTestRole3");
        dao.deleteRole(tenant, "NestedTestRole4");
    }
    
    /* ---------------------------------------------------------------------- */
    /* createRoles:                                                           */
    /* ---------------------------------------------------------------------- */
    private void createRoles() throws TapisException
    {
        SkRoleDao dao = new SkRoleDao();
        dao.createRole(tenant, user, "NestedTestRole1", "Role created by NestedRoleTest");
        dao.createRole(tenant, user, "NestedTestRole2", "Role created by NestedRoleTest");
        dao.createRole(tenant, user, "NestedTestRole3", "Role created by NestedRoleTest");
        dao.createRole(tenant, user, "NestedTestRole4", "Role created by NestedRoleTest");
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignChildrenRoles:                                                   */
    /* ---------------------------------------------------------------------- */
    private void assignChildrenRoles() throws TapisException
    {
        // Get the roles.
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        
        // Create the role hierarchy with roles 3 and 4 as leaves.
        role1.addChildRole(user, "NestedTestRole2");
        role2.addChildRole(user, "NestedTestRole3");
        role2.addChildRole(user, "NestedTestRole4");
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkRoleDescendants:                                                  */
    /* ---------------------------------------------------------------------- */
    private void checkRoleDescendants() throws TapisException
    {
        // Get the roles.
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        
        // Get the descendants of each role.
        System.out.println();
        List<String> children1 = role1.getDescendantRoleNames();
        System.out.println(" **** role1 contains roles: " + Arrays.toString(children1.toArray()));
        List<String> children2 = role2.getDescendantRoleNames();
        System.out.println(" **** role2 contains roles: " + Arrays.toString(children2.toArray()));
        List<String> children3 = role3.getDescendantRoleNames();
        System.out.println(" **** role3 contains roles: " + Arrays.toString(children3.toArray()));
        List<String> children4 = role4.getDescendantRoleNames();
        System.out.println(" **** role4 contains roles: " + Arrays.toString(children4.toArray()));
    }

    /* ---------------------------------------------------------------------- */
    /* checkRoleAncestors:                                                    */
    /* ---------------------------------------------------------------------- */
    private void checkRoleAncestors() throws TapisException
    {
        // Get the roles.
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        
        // Get the descendants of each role.
        System.out.println();
        List<String> ancestor1 = role1.getAncestorRoleNames();
        System.out.println(" **** role1 ancestor roles: " + Arrays.toString(ancestor1.toArray()));
        List<String> ancestor2 = role2.getAncestorRoleNames();
        System.out.println(" **** role2 ancestor roles: " + Arrays.toString(ancestor2.toArray()));
        List<String> ancestor3 = role3.getAncestorRoleNames();
        System.out.println(" **** role3 ancestor roles: " + Arrays.toString(ancestor3.toArray()));
        List<String> ancestor4 = role4.getAncestorRoleNames();
        System.out.println(" **** role4 ancestor roles: " + Arrays.toString(ancestor4.toArray()));
    }

    /* ---------------------------------------------------------------------- */
    /* createPermissions:                                                     */
    /* ---------------------------------------------------------------------- */
    private void createPermissions() throws TapisException
    {
        SkPermissionDao dao = new SkPermissionDao();
        dao.createPermission(tenant, user, "NestedTestPerm1", "Permission created by NestedRoleTest");
        dao.createPermission(tenant, user, "NestedTestPerm2", "Permission created by NestedRoleTest");
        dao.createPermission(tenant, user, "NestedTestPerm3", "Permission created by NestedRoleTest");
        dao.createPermission(tenant, user, "NestedTestPerm4", "Permission created by NestedRoleTest");
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignRolePermissions:                                                 */
    /* ---------------------------------------------------------------------- */
    private void assignRolePermissions() throws TapisException
    {
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        role1.addPermission(user, "NestedTestPerm1");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        role2.addPermission(user, "NestedTestPerm2");
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        role3.addPermission(user, "NestedTestPerm3");
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        role4.addPermission(user, "NestedTestPerm4");
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkRolePermissions:                                                  */
    /* ---------------------------------------------------------------------- */
    private void checkRolePermissions() throws TapisException
    {
        // Get the roles.
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        
        // Get the descendants of each role.
        System.out.println();
        List<String> perms1 = role1.getTransitivePermissionNames();
        System.out.println(" **** role1 permissions: " + Arrays.toString(perms1.toArray()));
        List<String> perms2 = role2.getTransitivePermissionNames();
        System.out.println(" **** role2 permissions: " + Arrays.toString(perms2.toArray()));
        List<String> perms3 = role3.getTransitivePermissionNames();
        System.out.println(" **** role3 permissions: " + Arrays.toString(perms3.toArray()));
        List<String> perms4 = role4.getTransitivePermissionNames();
        System.out.println(" **** role4 permissions: " + Arrays.toString(perms4.toArray()));
    }

    /* ---------------------------------------------------------------------- */
    /* assignUserRoles:                                                       */
    /* ---------------------------------------------------------------------- */
    private void assignUserRoles() throws TapisException
    {
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        role1.addUser(user, user1);
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        role2.addUser(user, user2);
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        role3.addUser(user, user3);
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        role4.addUser(user, user4);
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignUserRoles:                                                       */
    /* ---------------------------------------------------------------------- */
    private void checkUserRoles() throws TapisException
    {
       SkUserRoleDao dao = new SkUserRoleDao();
       List<String> roles1 = dao.getUserRoleNames(tenant, user1);
       System.out.println(" **** user1 roles: " + Arrays.toString(roles1.toArray()));
       List<String> roles2 = dao.getUserRoleNames(tenant, user2);
       System.out.println(" **** user2 roles: " + Arrays.toString(roles2.toArray()));
       List<String> roles3 = dao.getUserRoleNames(tenant, user3);
       System.out.println(" **** user3 roles: " + Arrays.toString(roles3.toArray()));
       List<String> roles4 = dao.getUserRoleNames(tenant, user4);
       System.out.println(" **** user4 roles: " + Arrays.toString(roles4.toArray()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkUserPermissions:                                                  */
    /* ---------------------------------------------------------------------- */
    private void checkUserPermissions() throws TapisException
    {
        SkUserRoleDao dao = new SkUserRoleDao();
        List<String> perms1 = dao.getUserPermissionNames(tenant, user1);
        System.out.println(" **** user1 perms: " + Arrays.toString(perms1.toArray()));
        List<String> perms2 = dao.getUserPermissionNames(tenant, user2);
        System.out.println(" **** user2 perms: " + Arrays.toString(perms2.toArray()));
        List<String> perms3 = dao.getUserPermissionNames(tenant, user3);
        System.out.println(" **** user3 perms: " + Arrays.toString(perms3.toArray()));
        List<String> perms4 = dao.getUserPermissionNames(tenant, user4);
        System.out.println(" **** user4 perms: " + Arrays.toString(perms4.toArray()));
    }
}

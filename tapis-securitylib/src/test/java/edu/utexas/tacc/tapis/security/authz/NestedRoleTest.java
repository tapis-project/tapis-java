package edu.utexas.tacc.tapis.security.authz;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.security.authz.dao.SkRoleDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkUserRoleDao;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/** This test exercises the main capabilities of the security kernel's
 * authorization management.  Four of each of roles, permissions and users are
 * defined.  The role2 is a child of role1; role3 and role4 are both children
 * of role2.  The test checks that this hierarchical relationship between roles
 * is correctly implemented. 
 * 
 * @author rcardone
 */
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
        
        // Assign permissions to roles.
        assignRolePermissions();
        
        // Query role permission names.
        checkRolePermissionNames();
        
        // Query role permissions.
        checkRolePermissions();
        
        // Assign roles to users.
        assignUserRoles();
        
        // Query user roles.
        checkUserRoles();
        
        // Query user permission names.
        checkUserPermissions();
        
        // Delete roles created by prior runs of this test.
        deleteRoles();
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
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
        String creator = "NestedRoleTest";
        int rows = dao.createRole("NestedTestRole1", tenant, "Role created by NestedRoleTest", creator, tenant);
        rows = dao.createRole("NestedTestRole2", tenant, "Role created by NestedRoleTest", creator, tenant);
        rows = dao.createRole("NestedTestRole3", tenant, "Role created by NestedRoleTest", creator, tenant);
        rows = dao.createRole("NestedTestRole4", tenant, "Role created by NestedRoleTest", creator, tenant);
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
        int rows = role1.addChildRole(user, tenant, "NestedTestRole2");
        rows = role2.addChildRole(user, tenant, "NestedTestRole3");
        rows = role2.addChildRole(user, tenant, "NestedTestRole4");
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
        Assert.assertEquals(children1.contains("NestedTestRole2"), true);
        Assert.assertEquals(children1.contains("NestedTestRole3"), true);
        Assert.assertEquals(children1.contains("NestedTestRole4"), true);
        Assert.assertEquals(children1.size(), 3);
        
        List<String> children2 = role2.getDescendantRoleNames();
        System.out.println(" **** role2 contains roles: " + Arrays.toString(children2.toArray()));
        Assert.assertEquals(children2.contains("NestedTestRole3"), true);
        Assert.assertEquals(children2.contains("NestedTestRole4"), true);
        Assert.assertEquals(children2.size(), 2);
        
        List<String> children3 = role3.getDescendantRoleNames();
        System.out.println(" **** role3 contains roles: " + Arrays.toString(children3.toArray()));
        Assert.assertEquals(children3.isEmpty(), true);
        
        List<String> children4 = role4.getDescendantRoleNames();
        System.out.println(" **** role4 contains roles: " + Arrays.toString(children4.toArray()));
        Assert.assertEquals(children4.isEmpty(), true);
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
        Assert.assertEquals(ancestor1.isEmpty(), true);
        
        List<String> ancestor2 = role2.getAncestorRoleNames();
        System.out.println(" **** role2 ancestor roles: " + Arrays.toString(ancestor2.toArray()));
        Assert.assertEquals(ancestor2.contains("NestedTestRole1"), true);
        Assert.assertEquals(ancestor2.size(), 1);
        
        List<String> ancestor3 = role3.getAncestorRoleNames();
        System.out.println(" **** role3 ancestor roles: " + Arrays.toString(ancestor3.toArray()));
        Assert.assertEquals(ancestor3.contains("NestedTestRole1"), true);
        Assert.assertEquals(ancestor3.contains("NestedTestRole2"), true);
        Assert.assertEquals(ancestor3.size(), 2);
        
        List<String> ancestor4 = role4.getAncestorRoleNames();
        System.out.println(" **** role4 ancestor roles: " + Arrays.toString(ancestor4.toArray()));
        Assert.assertEquals(ancestor4.contains("NestedTestRole1"), true);
        Assert.assertEquals(ancestor4.contains("NestedTestRole2"), true);
        Assert.assertEquals(ancestor4.size(), 2);
    }

    /* ---------------------------------------------------------------------- */
    /* assignRolePermissions:                                                 */
    /* ---------------------------------------------------------------------- */
    private void assignRolePermissions() throws TapisException
    {
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        int rows = role1.addPermission(user, tenant, "fake:*:read");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        rows = role2.addPermission(user, tenant, "fake:a:read");
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        rows = role3.addPermission(user, tenant, "fake:b:read");
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        rows = role4.addPermission(user, tenant, "fake:c:read");
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkRolePermissionNames:                                              */
    /* ---------------------------------------------------------------------- */
    private void checkRolePermissionNames() throws TapisException
    {
        // Get the roles.
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        
        // Get the descendants of each role.
        System.out.println();
        List<String> perms1 = role1.getTransitivePermissions();
        System.out.println(" **** role1 permission names: " + Arrays.toString(perms1.toArray()));
        Assert.assertEquals(perms1.contains("fake:*:read"), true);
        Assert.assertEquals(perms1.contains("fake:a:read"), true);
        Assert.assertEquals(perms1.contains("fake:b:read"), true);
        Assert.assertEquals(perms1.contains("fake:c:read"), true);
        Assert.assertEquals(perms1.size(), 4);
        
        List<String> perms2 = role2.getTransitivePermissions();
        System.out.println(" **** role2 permission names: " + Arrays.toString(perms2.toArray()));
        Assert.assertEquals(perms2.contains("fake:a:read"), true);
        Assert.assertEquals(perms2.contains("fake:b:read"), true);
        Assert.assertEquals(perms2.contains("fake:c:read"), true);
        Assert.assertEquals(perms2.size(), 3);
        
        List<String> perms3 = role3.getTransitivePermissions();
        System.out.println(" **** role3 permission names: " + Arrays.toString(perms3.toArray()));
        Assert.assertEquals(perms3.contains("fake:b:read"), true);
        Assert.assertEquals(perms3.size(), 1);
       
        List<String> perms4 = role4.getTransitivePermissions();
        System.out.println(" **** role4 permission names: " + Arrays.toString(perms4.toArray()));
        Assert.assertEquals(perms4.contains("fake:c:read"), true);
        Assert.assertEquals(perms4.size(), 1);
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
        List<String> perms1 = role1.getTransitivePermissions();
        System.out.println(" **** role1 permissions: " + Arrays.toString(perms1.toArray()));
        Assert.assertEquals(perms1.contains("fake:*:read"), true);
        Assert.assertEquals(perms1.contains("fake:a:read"), true);
        Assert.assertEquals(perms1.contains("fake:b:read"), true);
        Assert.assertEquals(perms1.contains("fake:c:read"), true);
        Assert.assertEquals(perms1.size(), 4);
        
        List<String> perms2 = role2.getTransitivePermissions();
        System.out.println(" **** role2 permissions: " + Arrays.toString(perms2.toArray()));
        Assert.assertEquals(perms2.contains("fake:a:read"), true);
        Assert.assertEquals(perms2.contains("fake:b:read"), true);
        Assert.assertEquals(perms2.contains("fake:c:read"), true);
        Assert.assertEquals(perms2.size(), 3);
        
        List<String> perms3 = role3.getTransitivePermissions();
        System.out.println(" **** role3 permissions: " + Arrays.toString(perms3.toArray()));
        Assert.assertEquals(perms3.contains("fake:b:read"), true);
        Assert.assertEquals(perms3.size(), 1);
       
        List<String> perms4 = role4.getTransitivePermissions();
        System.out.println(" **** role4 permissions: " + Arrays.toString(perms4.toArray()));
        Assert.assertEquals(perms4.contains("fake:c:read"), true);
        Assert.assertEquals(perms4.size(), 1);
    }

    /* ---------------------------------------------------------------------- */
    /* assignUserRoles:                                                       */
    /* ---------------------------------------------------------------------- */
    private void assignUserRoles() throws TapisException
    {
        SkRoleDao dao = new SkRoleDao();
        SkRole role1 = dao.getRole(tenant, "NestedTestRole1");
        int rows = role1.addUser(user, tenant, user1, tenant);
        SkRole role2 = dao.getRole(tenant, "NestedTestRole2");
        rows = role2.addUser(user, tenant, user2, tenant);
        SkRole role3 = dao.getRole(tenant, "NestedTestRole3");
        rows = role3.addUser(user, tenant, user3, tenant);
        SkRole role4 = dao.getRole(tenant, "NestedTestRole4");
        rows = role4.addUser(user, tenant, user4, tenant);
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkUserRoles:                                                        */
    /* ---------------------------------------------------------------------- */
    private void checkUserRoles() throws TapisException
    {
       SkUserRoleDao dao = new SkUserRoleDao();
       List<String> roles1 = dao.getUserRoleNames(tenant, user1);
       System.out.println(" **** user1 roles: " + Arrays.toString(roles1.toArray()));
       Assert.assertEquals(roles1.contains("NestedTestRole1"), true);
       Assert.assertEquals(roles1.contains("NestedTestRole2"), true);
       Assert.assertEquals(roles1.contains("NestedTestRole3"), true);
       Assert.assertEquals(roles1.contains("NestedTestRole4"), true);
       Assert.assertEquals(roles1.size(), 4);
       
       List<String> roles2 = dao.getUserRoleNames(tenant, user2);
       System.out.println(" **** user2 roles: " + Arrays.toString(roles2.toArray()));
       Assert.assertEquals(roles2.contains("NestedTestRole2"), true);
       Assert.assertEquals(roles2.contains("NestedTestRole3"), true);
       Assert.assertEquals(roles2.contains("NestedTestRole4"), true);
       Assert.assertEquals(roles2.size(), 3);
      
       List<String> roles3 = dao.getUserRoleNames(tenant, user3);
       System.out.println(" **** user3 roles: " + Arrays.toString(roles3.toArray()));
       Assert.assertEquals(roles3.contains("NestedTestRole3"), true);
       Assert.assertEquals(roles3.size(), 1);
       
       List<String> roles4 = dao.getUserRoleNames(tenant, user4);
       System.out.println(" **** user4 roles: " + Arrays.toString(roles4.toArray()));
       Assert.assertEquals(roles4.contains("NestedTestRole4"), true);
       Assert.assertEquals(roles4.size(), 1);
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkUserPermissions:                                                  */
    /* ---------------------------------------------------------------------- */
    private void checkUserPermissions() throws TapisException
    {
        SkUserRoleDao dao = new SkUserRoleDao();
        List<String> perms1 = dao.getUserPermissions(tenant, user1);
        System.out.println(" **** user1 perms: " + Arrays.toString(perms1.toArray()));
        Assert.assertEquals(perms1.contains("fake:*:read"), true);
        Assert.assertEquals(perms1.contains("fake:a:read"), true);
        Assert.assertEquals(perms1.contains("fake:b:read"), true);
        Assert.assertEquals(perms1.contains("fake:c:read"), true);
        Assert.assertEquals(perms1.size(), 4);
       
        List<String> perms2 = dao.getUserPermissions(tenant, user2);
        System.out.println(" **** user2 perms: " + Arrays.toString(perms2.toArray()));
        Assert.assertEquals(perms2.contains("fake:a:read"), true);
        Assert.assertEquals(perms2.contains("fake:b:read"), true);
        Assert.assertEquals(perms2.contains("fake:c:read"), true);
        Assert.assertEquals(perms2.size(), 3);
        
        List<String> perms3 = dao.getUserPermissions(tenant, user3);
        System.out.println(" **** user3 perms: " + Arrays.toString(perms3.toArray()));
        Assert.assertEquals(perms3.contains("fake:b:read"), true);
        Assert.assertEquals(perms3.size(), 1);
        
        List<String> perms4 = dao.getUserPermissions(tenant, user4);
        System.out.println(" **** user4 perms: " + Arrays.toString(perms4.toArray()));
        Assert.assertEquals(perms4.contains("fake:c:read"), true);
        Assert.assertEquals(perms4.size(), 1);
    }
}

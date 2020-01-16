package edu.utexas.tacc.tapis.security.authz.permissions;

import java.util.List;

import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.authz.impl.UserImpl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;

@Test(groups={"integration"})
public class UserPermsTest 
{
    // Fields.
    private UserImpl _userImpl;
    private RoleImpl _roleImpl;
    
    /* ---------------------------------------------------------------------- */
    /* setup:                                                                 */
    /* ---------------------------------------------------------------------- */
    @BeforeSuite
    public void setup()
    {
        // Disable vault so we only test the db login.
        System.setProperty("tapis.sk.vault.disable", "true");
        
        // Access requires a database connection.
        _userImpl = UserImpl.getInstance();
        _roleImpl = RoleImpl.getInstance();
    }
    
    /* ---------------------------------------------------------------------- */
    /* permsTest:                                                             */
    /* ---------------------------------------------------------------------- */
    public void permsTest() throws TapisImplException, TapisNotFoundException
    {
        // Add a bunch of permissions to user bobby.
        _userImpl.grantUserPermission("dev", "admin", "bobby", "stream:dev:read:project1");
        _userImpl.grantUserPermission("dev", "admin", "bobby", "stream:dev:read,write:project1");
        _userImpl.grantUserPermission("dev", "admin", "bobby", "stream:dev:read,write,exec:project1");
        
        // List all of bobby's permissions.
        List<String> perms = _userImpl.getUserPerms("dev", "bobby", null, null);
        System.out.println("All permissions:");
        for (String perm : perms) System.out.println("    " + perm);
        
        // List a subset of bobby's permissions.
        perms = _userImpl.getUserPerms("dev", "bobby", "stream:dev:*:project1", null);
        System.out.println("\n\"stream:dev:*:project1\" IMPLIES:");
        for (String perm : perms) System.out.println("    " + perm);
        
        // List a subset of bobby's permissions.
        perms = _userImpl.getUserPerms("dev", "bobby", "stream:dev:write:project1", null);
        System.out.println("\n\"stream:dev:write:project1\" IMPLIES:");
        for (String perm : perms) System.out.println("    " + perm);
        
        // List a subset of bobby's permissions.
        perms = _userImpl.getUserPerms("dev", "bobby", null, "stream:dev:*:project1");
        System.out.println("\n\"stream:dev:*:project1\" IMPLIED BY:");
        for (String perm : perms) System.out.println("    " + perm);
        
        // List a subset of bobby's permissions.
        perms = _userImpl.getUserPerms("dev", "bobby", null, "stream:dev:write:project1");
        System.out.println("\n\"stream:dev:write:project1\" IMPLIED BY:");
        for (String perm : perms) System.out.println("    " + perm);

        // Optional clean up.
//        _roleImpl.removeRolePermission("dev", "$$bobby", "stream:dev:read:project1");
//        _roleImpl.removeRolePermission("dev", "$$bobby", "stream:dev:read,write:project1");
//        _roleImpl.removeRolePermission("dev", "$$bobby", "stream:dev:read,write,exec:project1");
    }
}

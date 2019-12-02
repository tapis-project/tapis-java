package edu.utexas.tacc.tapis.security.authz.permissions;

import org.testng.Assert;
import org.testng.annotations.Test;

/** Basic sanity check of Shiro Wildcard permission checking.
 * 
 * @author rcardone
 */
@Test(groups= {"unit"})
public class ShiroExtPermissionTest 
{
    /* ---------------------------------------------------------------------- */
    /* tenantTest:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void tenantTest()
    {
      // The required permission spec.
      ExtWildcardPermission wcPerm = new ExtWildcardPermission("files:iplantc.org");
      
      boolean implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/myfile"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/myfile");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/myfile"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/myfile");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/my:file"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/my:file");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/my,file"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/my,file");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/my*file"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/my*file");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/*,:"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/*,:");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/:xx"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/:xx");
    }

    /* ---------------------------------------------------------------------- */
    /* pathTest:                                                              */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void pathTest()
    {
      // The required permission spec.
      ExtWildcardPermission wcPerm = new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud");
      
      boolean implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/myfile"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/myfile");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/myfile"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/myfile");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/my:file"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/my:file");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/my,file"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/my,file");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/my*file"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/my*file");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/*,:"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/*,:");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/:xx"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/:xx");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to NOT imply files:iplantc.org");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to NOT imply files:iplantc.org:read");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to NOT imply files:iplantc.org:read:stampede2");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read,write:stampede2:/home/bud/myfile"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to NOT imply files:iplantc.org:read,write:stampede2:/home/bud/myfile");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:write:stampede2:/home/bud/myfile"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to NOT imply files:iplantc.org:write:stampede2:/home/bud/myfile");
      
      implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:*:/home/bud/mydir/myfile"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to NOT imply files:iplantc.org:read:*:/home/bud/mydir/myfile");
      
    }

    /* ---------------------------------------------------------------------- */
    /* validPermTest:                                                         */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void validPermTest()
    {
        // We accept goofy parts that include an asterisk.  
        // This is really the same as "files:*" or "files".
        ExtWildcardPermission wcPerm = new ExtWildcardPermission("files:b,*");
        
        boolean implies = wcPerm.implies(new ExtWildcardPermission("files:b"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:b");
        
        implies = wcPerm.implies(new ExtWildcardPermission("files:z"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:z");
        
        implies = wcPerm.implies(new ExtWildcardPermission("files"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files");
        
        implies = wcPerm.implies(new ExtWildcardPermission("files:*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:*");
        
        implies = wcPerm.implies(new ExtWildcardPermission("files:b,*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:b,*");
        
        implies = wcPerm.implies(new ExtWildcardPermission("files:c,*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:c,*");
        
        implies = wcPerm.implies(new ExtWildcardPermission("files:*:d:e:f"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:*:d:e:f");
        
        implies = wcPerm.implies(new ExtWildcardPermission("*"));
        Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply *");
    }
    
    /* ---------------------------------------------------------------------- */
    /* allowAllTest:                                                          */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void allowAllTest()
    {
        // We accept everything.
        ExtWildcardPermission wcPerm = new ExtWildcardPermission("*");
        
        boolean implies = wcPerm.implies(new ExtWildcardPermission("files:iplantc.org:read:stampede2:/home/bud/mydir/myfile"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply files:iplantc.org:read:stampede2:/home/bud/mydir/myfile");
    }
}

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
    /* simpleTest:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test
    public void simpleTest()
    {
      // The required permission spec.
      ExtWildcardPermission wcPerm = new ExtWildcardPermission("a:b:c,d");
      
      boolean implies = wcPerm.implies(new ExtWildcardPermission("a:b:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:c");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b:d"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d:e:f");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b:z"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:b:z");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:b");

      implies = wcPerm.implies(new ExtWildcardPermission("*:b:d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to imply *:b:d");
      
      implies = wcPerm.implies(new ExtWildcardPermission("x"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply x");
    }

    /* ---------------------------------------------------------------------- */
    /* wildcardAtEndTest:                                                     */
    /* ---------------------------------------------------------------------- */
    @Test
    public void wildcardAtEndTest()
    {
      // The required permission spec.
      ExtWildcardPermission wcPerm = new ExtWildcardPermission("a:b:*");
      
      boolean implies = wcPerm.implies(new ExtWildcardPermission("a:b:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:c");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d:e:f");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:*"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:*");
      
      implies = wcPerm.implies(new ExtWildcardPermission("x"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply x");
    }

    /* ---------------------------------------------------------------------- */
    /* wildcardInMiddleTest:                                                  */
    /* ---------------------------------------------------------------------- */
    @Test
    public void wildcardInMiddleTest()
    {
      // The required permission spec.
      ExtWildcardPermission wcPerm = new ExtWildcardPermission("a:*:c,d");
      
      boolean implies = wcPerm.implies(new ExtWildcardPermission("a:b:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:c");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:banana:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:banana:c");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:*:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:c");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:*:d"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:d");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d:e:f");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:*:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:d:e:f");
      
      // Both c AND d must be true.
      implies = wcPerm.implies(new ExtWildcardPermission("a:p:c,d"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:p:c,d");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:*:v"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:*:v");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:banana"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply :banana");
      
      implies = wcPerm.implies(new ExtWildcardPermission("a:b"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:b");
      
      implies = wcPerm.implies(new ExtWildcardPermission("*:*:d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply *:*:d");
      
      implies = wcPerm.implies(new ExtWildcardPermission("*:b:d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply *:b:d");
      
      implies = wcPerm.implies(new ExtWildcardPermission("x"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply x");
      
      // Comma is a logical AND when appearing in a request.
      implies = wcPerm.implies(new ExtWildcardPermission("a:p:c,e"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:p:c,e");
      
      // Comma is a logical AND when appearing in a request.
      implies = wcPerm.implies(new ExtWildcardPermission("a:p:c,d,e"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:p:c,d,e");
      
      // Comma is a logical AND when appearing in a request.
      implies = wcPerm.implies(new ExtWildcardPermission("a,q:p:c,d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a,q:p:c,d");
   }

    /* ---------------------------------------------------------------------- */
    /* validPermTest:                                                         */
    /* ---------------------------------------------------------------------- */
    @Test
    public void validPermTest()
    {
        // We accept goofy parts that include an asterisk.  
        // This is really the same as "a:*" or "a".
        ExtWildcardPermission wcPerm = new ExtWildcardPermission("a:b,*");
        
        boolean implies = wcPerm.implies(new ExtWildcardPermission("a:b"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b");
        
        implies = wcPerm.implies(new ExtWildcardPermission("a:z"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:z");
        
        implies = wcPerm.implies(new ExtWildcardPermission("a"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a");
        
        implies = wcPerm.implies(new ExtWildcardPermission("a"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*");
        
        implies = wcPerm.implies(new ExtWildcardPermission("a:b,*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b,*");
        
        implies = wcPerm.implies(new ExtWildcardPermission("a:c,*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:c,*");
        
        implies = wcPerm.implies(new ExtWildcardPermission("a:*:d:e:f"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:d:e:f");
        
        implies = wcPerm.implies(new ExtWildcardPermission("*"));
        Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply *");
    }
}

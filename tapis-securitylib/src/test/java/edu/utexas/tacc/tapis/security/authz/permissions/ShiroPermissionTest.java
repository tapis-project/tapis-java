package edu.utexas.tacc.tapis.security.authz.permissions;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Basic sanity check of standard Shiro Wildcard permission checking using
 * the extended wildcard class.
 * 
 * @author rcardone
 */
@Test(groups= {"unit"})
public class ShiroPermissionTest 
{
    /* ---------------------------------------------------------------------- */
    /* simpleTest:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test
    public void simpleTest()
    {
      // The required permission spec.
      WildcardPermission wcPerm = new WildcardPermission("a:b:c,d");
      
      boolean implies = wcPerm.implies(new WildcardPermission("a:b:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:c");
      
      implies = wcPerm.implies(new WildcardPermission("a:b:d"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d");
      
      implies = wcPerm.implies(new WildcardPermission("a:b:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d:e:f");
      
      implies = wcPerm.implies(new WildcardPermission("a:b:z"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:b:z");
      
      implies = wcPerm.implies(new WildcardPermission("a:b"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:b");

      implies = wcPerm.implies(new WildcardPermission("*:b:d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to imply *:b:d");
      
      implies = wcPerm.implies(new WildcardPermission("x"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply x");
    }

    /* ---------------------------------------------------------------------- */
    /* wildcardAtEndTest:                                                     */
    /* ---------------------------------------------------------------------- */
    @Test
    public void wildcardAtEndTest()
    {
      // The required permission spec.
      WildcardPermission wcPerm = new WildcardPermission("a:b:*");
      
      boolean implies = wcPerm.implies(new WildcardPermission("a:b:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:c");
      
      implies = wcPerm.implies(new WildcardPermission("a:b"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b");
      
      implies = wcPerm.implies(new WildcardPermission("a:b:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d:e:f");
      
      implies = wcPerm.implies(new WildcardPermission("a"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a");
      
      implies = wcPerm.implies(new WildcardPermission("a:*"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:*");
      
      implies = wcPerm.implies(new WildcardPermission("x"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply x");
    }

    /* ---------------------------------------------------------------------- */
    /* wildcardInMiddleTest:                                                  */
    /* ---------------------------------------------------------------------- */
    @Test
    public void wildcardInMiddleTest()
    {
      // The required permission spec.
      WildcardPermission wcPerm = new WildcardPermission("a:*:c,d");
      
      boolean implies = wcPerm.implies(new WildcardPermission("a:b:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:c");
      
      implies = wcPerm.implies(new WildcardPermission("a:banana:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:banana:c");
      
      implies = wcPerm.implies(new WildcardPermission("a:*:c"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:c");
      
      implies = wcPerm.implies(new WildcardPermission("a:*:d"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:d");
      
      implies = wcPerm.implies(new WildcardPermission("a:b:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b:d:e:f");
      
      implies = wcPerm.implies(new WildcardPermission("a:*:d:e:f"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:d:e:f");
      
      // Both c AND d must be true.
      implies = wcPerm.implies(new WildcardPermission("a:p:c,d"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:p:c,d");
      
      implies = wcPerm.implies(new WildcardPermission("a:*:v"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:*:v");
      
      implies = wcPerm.implies(new WildcardPermission("a:banana"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply :banana");
      
      implies = wcPerm.implies(new WildcardPermission("a:b"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:b");
      
      implies = wcPerm.implies(new WildcardPermission("*:*:d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply *:*:d");
      
      implies = wcPerm.implies(new WildcardPermission("*:b:d"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply *:b:d");
      
      implies = wcPerm.implies(new WildcardPermission("x"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply x");
      
      // Comma is a logical AND when appearing in a request.
      implies = wcPerm.implies(new WildcardPermission("a:p:c,e"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:p:c,e");
      
      // Comma is a logical AND when appearing in a request.
      implies = wcPerm.implies(new WildcardPermission("a:p:c,d,e"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply a:p:c,d,e");
      
      // Comma is a logical AND when appearing in a request.
      implies = wcPerm.implies(new WildcardPermission("a,q:p:c,d"));
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
        WildcardPermission wcPerm = new WildcardPermission("a:b,*");
        
        boolean implies = wcPerm.implies(new WildcardPermission("a:b"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b");
        
        implies = wcPerm.implies(new WildcardPermission("a:z"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:z");
        
        implies = wcPerm.implies(new WildcardPermission("a"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a");
        
        implies = wcPerm.implies(new WildcardPermission("a:*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*");
        
        implies = wcPerm.implies(new WildcardPermission("a:b,*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:b,*");
        
        implies = wcPerm.implies(new WildcardPermission("a:c,*"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:c,*");
        
        implies = wcPerm.implies(new WildcardPermission("a:*:d:e:f"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:*:d:e:f");
        
        implies = wcPerm.implies(new WildcardPermission("*"));
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
        
        boolean implies = wcPerm.implies(new WildcardPermission("a:p:c,d"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply a:p:c,d");
    }
    
    /* ---------------------------------------------------------------------- */
    /* DBTest:                                                                */
    /* ---------------------------------------------------------------------- */
    @Test
    public void DBTest()
    {
      // The required permission spec.
      WildcardPermission wcPerm = new WildcardPermission("meta:dev:GET,POST,PUT:streamdb");
      
      boolean implies = wcPerm.implies(new WildcardPermission("meta:dev:GET:streamdb:coll1"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:GET:streamdb:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:POST:streamdb:coll1"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:POST:streamdb:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:PUT:streamdb:coll1"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:PUT:streamdb:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb:coll1"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DEL:streamdb:coll1");
      
      // New perm.
      wcPerm = new WildcardPermission("meta:dev:DEL:streamdb:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb:coll1"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:DEL:streamdb:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DEL:streamdb");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:GET:streamdb:coll1"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:GET:streamdb:coll1");
      
      // New perm.
      wcPerm = new WildcardPermission("meta:dev:DEL:streamdb:collection");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb:collection:coll1"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:DEL:streamdb:collection:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb:collection"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:DEL:streamdb:collection");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb:collection:*"));
      Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:DEL:streamdb:collection:*");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DEL:streamdb");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb:coll1"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DEL:streamdb:coll1");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:DEL:streamdb"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DEL:streamdb");
      
      implies = wcPerm.implies(new WildcardPermission("meta:dev:GET:streamdb:coll1"));
      Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:GET:streamdb:coll1");
    }

    /* ---------------------------------------------------------------------- */
    /* AnotherTest:                                                           */
    /* ---------------------------------------------------------------------- */
    @Test
    public void AnotherTest()
    {
        // The required permission spec.
        WildcardPermission wcPerm = new WildcardPermission("meta:dev:DELETE:slttestdb:somecollection:*");
        
        boolean implies = wcPerm.implies(new WildcardPermission("meta:dev:DELETE:slttestdb:*"));
        Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DELETE:slttestdb:*");
        
        implies = wcPerm.implies(new WildcardPermission("meta:dev:DELETE:slttestdb"));
        Assert.assertFalse(implies, "Expected " + wcPerm + " to not imply meta:dev:DELETE:slttestdb");
        
        implies = wcPerm.implies(new WildcardPermission("meta:dev:DELETE:slttestdb:somecollection"));
        Assert.assertTrue(implies, "Expected " + wcPerm + " to imply meta:dev:DELETE:slttestdb:somecollection");
    }    
    
}
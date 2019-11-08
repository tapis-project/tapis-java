package edu.utexas.tacc.tapis.security.client;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.security.client.gen.model.ResultChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultNameArray;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultResourceUrl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class SKClientTest 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // A hardcoded JWT with a 10 year expiration.  Here's the curl command that 
    // created the JWT:
    //
    //  curl -H "Content-type: application/json" -d '{"token_tenant_id": "dev", "token_type": "user", "token_username": "testuser2", "access_token_ttl": 315569520}'  'https://dev.develop.tapis.io/tokens'
    //
    private static final String JWT =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2Rldi5hcGkudGFwaXMuaW8vdG9rZW5zL3YzIiwic3ViIjoidGVzdHVzZXIyQGRldiIsInRhcGlzL3RlbmFudF9pZCI6ImRldiIsInRhcGlzL3Rva2VuX3R5cGUiOiJhY2Nlc3MiLCJ0YXBpcy9kZWxlZ2F0aW9uIjpmYWxzZSwidGFwaXMvZGVsZWdhdGlvbl9zdWIiOm51bGwsInRhcGlzL3VzZXJuYW1lIjoidGVzdHVzZXIyIiwidGFwaXMvYWNjb3VudF90eXBlIjoidXNlciIsImV4cCI6MTg4ODU1Njc1MX0.sC7uZAUpWYqiSuVEUWbHr6nWhHY2BCLQ0gBHCytaeGBPAbGb51g-vT2M_Y_r4JvEXD6m4HSYPmn3uHIkPYCK4JohNhTcq1iq-C4o-hnwS4MehQBm29-YHnMAOyPAaKqW8Uxt9rfbRwsLcfmQqG8U3BuasF4FzAQQ1PH8myMSG0BmaOEXOmx6tcrhlwGJ5HUCARRK1lUamKxmScj1QuGvn8Wljexv8ne0MdR4KscaCAyVTDBTOMzMPTH06G8ScZ14ecOZSBLnknI7fK1PWE39kEO9-eBln2uWpOHIg2twRPOZuy-lHic7aIBFzJhGI_J-ie_VBSzYTryNExGgGV9H1g";
    
    /* ---------------------------------------------------------------------- */
    /* testRole:                                                              */
    /* ---------------------------------------------------------------------- */
    @Test(enabled = true)
    public void testRole() throws TapisException
    {
        SKClient skClient = new SKClient(null, JWT);
        
        // Create a role.
        ResultResourceUrl resp1;
        try {resp1 = skClient.createRole("peachy", "This is a peachy description.");}
            catch (Exception e) {
                System.out.println(e.toString());
                throw e;
            }
        System.out.println("createRole: " + resp1 + "\n");
        
        // Delete a role.
        ResultChangeCount resp2;
        try {resp2 = skClient.deleteRoleByName("peachy");}
        catch (Exception e) {
            System.out.println(e.toString());
            throw e;
        }
        System.out.println("deleteRoleByName: " + resp2 + "\n");
    }

    /* ---------------------------------------------------------------------- */
    /* testGetUsersWithRole:                                                  */
    /* ---------------------------------------------------------------------- */
    @Test(enabled = true)
    public void testGetUsersWithRole() throws TapisException
    {
        SKClient skClient = new SKClient(null, JWT);
        String roleName = "piggy";
        
        // Create a role.
        ResultResourceUrl resp1;
        try {resp1 = skClient.createRole(roleName, "you little piggy.");}
            catch (Exception e) {
                System.out.println(e.toString());
                throw e;
            }
        System.out.println("createRole: " + resp1 + "\n");
        
        // Assign a user the role.
        ResultChangeCount resp2;
        try {resp2 = skClient.grantUserRole("bud", roleName);}
        catch (Exception e) {
            System.out.println(e.toString());
            throw e;
        }
        System.out.println("grantUserRole: " + resp2 + "\n");
        
        // Assign a user the role.
        ResultChangeCount resp3;
        try {resp3 = skClient.grantUserRole("jane", roleName);}
        catch (Exception e) {
            System.out.println(e.toString());
            throw e;
        }
        System.out.println("grantUserRole: " + resp3 + "\n");
        
        // Get users with role.
        ResultNameArray resp4;
        try {resp4 = skClient.getUsersWithRole(roleName);}
        catch (Exception e) {
            System.out.println(e.toString());
            throw e;
        }
        System.out.println("grantUserRole: " + resp4 + "\n");
        
        // Delete a role.
        ResultChangeCount resp5;
        try {resp5 = skClient.deleteRoleByName(roleName);}
        catch (Exception e) {
            System.out.println(e.toString());
            throw e;
        }
        System.out.println("deleteRoleByName: " + resp5 + "\n");
    }
}

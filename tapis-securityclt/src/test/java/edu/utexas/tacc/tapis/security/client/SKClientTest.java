package edu.utexas.tacc.tapis.security.client;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.security.client.gen.model.ResultChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultResourceUrl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class SKClientTest 
{
    /* ---------------------------------------------------------------------- */
    /* testRole:                                                              */
    /* ---------------------------------------------------------------------- */
    @Test
    public void testRole() throws TapisException
    {
        SKClient skClient = new SKClient(null);
        
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

}

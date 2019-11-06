package edu.utexas.tacc.tapis.tokens.client;

import org.testng.annotations.Test;

//import edu.utexas.tacc.tapis.tokens.client.gen.ApiException;
//import edu.utexas.tacc.tapis.tokens.client.gen.api.RoleApi;
//import edu.utexas.tacc.tapis.tokens.client.gen.model.ReqCreateRole;
//import edu.utexas.tacc.tapis.tokens.client.gen.model.RespChangeCount;
//import edu.utexas.tacc.tapis.tokens.client.gen.model.RespResourceUrl;
//import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

@Test(groups={"integration"})
public class TokClientTest
{
    /* ********************************************************************** */
    /*                            Main Test Method                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* testAll:                                                               */
    /* ---------------------------------------------------------------------- */
//    @Test
//    public void testRole() throws ApiException
//    {
//        // Get the API object using default networking.
//        RoleApi roleApi = new RoleApi();
//        
//        // Create a role.
//        ReqCreateRole body = new ReqCreateRole();
//        body.setRoleName("peachy");
//        body.setDescription("This is a peachy description.");
//        RespResourceUrl urlResp = roleApi.createRole(body,true);
//        System.out.println("createRole: " + urlResp + "\n");
//        System.out.println("createRole: " + urlResp.getResult().getUrl() + "\n");
//        
//        
//        RespChangeCount countResp = roleApi.deleteRoleByName("peachy", true);
//        System.out.println("deleteRoleByName: " + countResp + "\n");
//        
//        System.out.println(TapisGsonUtils.getGson(true).toJson(countResp));
//        
//    }
}

package edu.utexas.tacc.tapis.security.client;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.security.client.gen.ApiClient;
import edu.utexas.tacc.tapis.security.client.gen.ApiException;
import edu.utexas.tacc.tapis.security.client.gen.Configuration;
import edu.utexas.tacc.tapis.security.client.gen.api.RoleApi;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqCreateRole;
import edu.utexas.tacc.tapis.security.client.gen.model.RespChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.RespResourceUrl;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

@Test(groups={"integration"})
public class GenTest 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Header key for jwts.
    private static final String TAPIS_JWT_HEADER = "X-Tapis-Token";
    
    // A hardcoded JWT with a 10 year expiration.  Here's the curl command that 
    // created the JWT:
    //
    //  curl -H "Content-type: application/json" -d '{"token_tenant_id": "dev", "token_type": "user", "token_username": "testuser2", "access_token_ttl": 315569520}'  'https://dev.develop.tapis.io/tokens'
    //
    private static final String JWT =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2Rldi5hcGkudGFwaXMuaW8vdG9rZW5zL3YzIiwic3ViIjoidGVzdHVzZXIyQGRldiIsInRhcGlzL3RlbmFudF9pZCI6ImRldiIsInRhcGlzL3Rva2VuX3R5cGUiOiJhY2Nlc3MiLCJ0YXBpcy9kZWxlZ2F0aW9uIjpmYWxzZSwidGFwaXMvZGVsZWdhdGlvbl9zdWIiOm51bGwsInRhcGlzL3VzZXJuYW1lIjoidGVzdHVzZXIyIiwidGFwaXMvYWNjb3VudF90eXBlIjoidXNlciIsImV4cCI6MTg4ODU1Njc1MX0.sC7uZAUpWYqiSuVEUWbHr6nWhHY2BCLQ0gBHCytaeGBPAbGb51g-vT2M_Y_r4JvEXD6m4HSYPmn3uHIkPYCK4JohNhTcq1iq-C4o-hnwS4MehQBm29-YHnMAOyPAaKqW8Uxt9rfbRwsLcfmQqG8U3BuasF4FzAQQ1PH8myMSG0BmaOEXOmx6tcrhlwGJ5HUCARRK1lUamKxmScj1QuGvn8Wljexv8ne0MdR4KscaCAyVTDBTOMzMPTH06G8ScZ14ecOZSBLnknI7fK1PWE39kEO9-eBln2uWpOHIg2twRPOZuy-lHic7aIBFzJhGI_J-ie_VBSzYTryNExGgGV9H1g";
    
    /* ********************************************************************** */
    /*                            Main Test Method                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* testRole:                                                              */
    /* ---------------------------------------------------------------------- */
    @Test
    public void testRole() throws ApiException
    {
        // Get the API object using default networking.
        ApiClient apiClient = Configuration.getDefaultApiClient();
        apiClient.addDefaultHeader(TAPIS_JWT_HEADER, JWT);
        RoleApi roleApi = new RoleApi();
        
        // Create a role.
        ReqCreateRole body = new ReqCreateRole();
        body.setRoleName("peachy");
        body.setDescription("This is a peachy description.");
        RespResourceUrl urlResp = roleApi.createRole(body,true);
        System.out.println("createRole: " + urlResp + "\n");
        System.out.println("createRole: " + urlResp.getResult().getUrl() + "\n");
        
        
        RespChangeCount countResp = roleApi.deleteRoleByName("peachy", true);
        System.out.println("deleteRoleByName: " + countResp + "\n");
        
        System.out.println(TapisGsonUtils.getGson(true).toJson(countResp));
    }
}

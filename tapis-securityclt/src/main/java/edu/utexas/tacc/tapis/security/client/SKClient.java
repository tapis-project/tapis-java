package edu.utexas.tacc.tapis.security.client;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;

import edu.utexas.tacc.tapis.security.client.gen.ApiClient;
import edu.utexas.tacc.tapis.security.client.gen.ApiException;
import edu.utexas.tacc.tapis.security.client.gen.Configuration;
import edu.utexas.tacc.tapis.security.client.gen.api.RoleApi;
import edu.utexas.tacc.tapis.security.client.gen.api.UserApi;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqAddChildRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqAddRolePermission;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqCreateRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqGrantUserRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqGrantUserRoleWithPermission;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqRemoveChildRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqRemoveRolePermission;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqRemoveUserRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqReplacePathPrefix;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqUpdateRoleDescription;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqUpdateRoleName;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqUserHasRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqUserHasRoleMulti;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqUserIsPermitted;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqUserIsPermittedMulti;
import edu.utexas.tacc.tapis.security.client.gen.model.RespAuthorized;
import edu.utexas.tacc.tapis.security.client.gen.model.RespBasic;
import edu.utexas.tacc.tapis.security.client.gen.model.RespChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.RespNameArray;
import edu.utexas.tacc.tapis.security.client.gen.model.RespResourceUrl;
import edu.utexas.tacc.tapis.security.client.gen.model.RespRole;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultAuthorized;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultNameArray;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultResourceUrl;
import edu.utexas.tacc.tapis.security.client.gen.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public class SKClient 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Response status.
    public static final String STATUS_SUCCESS = "success";
    
    /* **************************************************************************** */
    /*                                     Enums                                    */
    /* **************************************************************************** */
    // Custom error messages that may be reported by methods.
    public enum EMsg {NO_RESPONSE, ERROR_STATUS, UNKNOWN_RESPONSE_TYPE}
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Response serializer.
    private static final Gson _gson = TapisGsonUtils.getGson();
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Constructor that uses the compiled-in basePath value in ApiClient.  This
     * constructor is only appropriate for test code.
     */
    public SKClient() {this(null);}
    
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Constructor that overrides the compiled-in basePath value in ApiClient.  This
     * constructor typically used in production.
     * 
     * The path includes the URL prefix up to and including the service root.  By
     * default this value is http://localhost:8080/security.  In production environments
     * the protocol is https and the host/port will be specific to that environment. 
     * 
     * @param path the base path 
     */
    public SKClient(String path) 
    {
        ApiClient apiClient = Configuration.getDefaultApiClient();
        if (!StringUtils.isBlank(path)) apiClient.setBasePath(path);
    }

    /* **************************************************************************** */
    /*                              Public Methods                                  */
    /* **************************************************************************** */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getApiClient: Return underlying ApiClient                                    */
    /* ---------------------------------------------------------------------------- */
    public ApiClient getApiClient()
    {
        return Configuration.getDefaultApiClient();
    }

    /* ---------------------------------------------------------------------------- */
    /* addDefaultHeader: Add http header to client                                  */
    /* ---------------------------------------------------------------------------- */
    public ApiClient addDefaultHeader(String key, String val)
    {
        return Configuration.getDefaultApiClient().addDefaultHeader(key, val);
    }


    /* **************************************************************************** */
    /*                              Public Role Methods                             */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getRoleNames:                                                                */
    /* ---------------------------------------------------------------------------- */
    public ResultNameArray getRoleNames()
     throws TapisClientException
    {
        // Make the REST call.
        RespNameArray resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.getRoleNames(false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getRoleByName:                                                               */
    /* ---------------------------------------------------------------------------- */
    public SkRole getRoleByName(String roleName)
     throws TapisClientException
    {
        // Make the REST call.
        RespRole resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.getRoleByName(roleName, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createRole:                                                                  */
    /* ---------------------------------------------------------------------------- */
    public ResultResourceUrl createRole(String roleName, String description)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqCreateRole();
        body.setRoleName(roleName);
        body.setDescription(description);
        
        // Make the REST call.
        RespResourceUrl resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.createRole(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* deleteRoleByName:                                                            */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount deleteRoleByName(String roleName)
     throws TapisClientException
    {
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.deleteRoleByName(roleName, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* updateRoleName:                                                              */
    /* ---------------------------------------------------------------------------- */
    public void updateRoleName(String roleName, String newRoleName)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUpdateRoleName();
        body.setNewRoleName(newRoleName);
        
        // Make the REST call.
        RespBasic resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.updateRoleName(body, roleName, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* updateRoleDescription:                                                       */
    /* ---------------------------------------------------------------------------- */
    public void updateRoleDescription(String roleName, String description)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUpdateRoleDescription();
        body.setDescription(description);
        
        // Make the REST call.
        RespBasic resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.updateRoleDescription(body, roleName, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* addRolePermission:                                                           */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount addRolePermission(String roleName, String permSpec)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqAddRolePermission();
        body.setRoleName(roleName);
        body.setPermSpec(permSpec);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.addRolePermission(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* addRemovePermission:                                                         */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount addRemovePermission(String roleName, String permSpec)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqRemoveRolePermission();
        body.setRoleName(roleName);
        body.setPermSpec(permSpec);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.removeRolePermission(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* addChildRole:                                                                */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount addChildRole(String parentRoleName, String childRoleName)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqAddChildRole();
        body.setParentRoleName(parentRoleName);
        body.setChildRoleName(childRoleName);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.addChildRole(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* removeChildRole:                                                             */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount removeChildRole(String parentRoleName, String childRoleName)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqRemoveChildRole();
        body.setParentRoleName(parentRoleName);
        body.setChildRoleName(childRoleName);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.removeChildRole(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* replacePathPrefix:                                                           */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount replacePathPrefix(String schema, String roleName,
                                               String oldSystemId, String newSystemId,
                                               String oldPrefix, String newPrefix)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqReplacePathPrefix();
        body.setSchema(schema);
        body.setRoleName(roleName);
        body.setOldSystemId(oldSystemId);
        body.setNewSystemId(newSystemId);
        body.setOldPrefix(oldPrefix);
        body.setNewPrefix(newPrefix);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            RoleApi roleApi = new RoleApi();
            resp = roleApi.replacePathPrefix(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* **************************************************************************** */
    /*                              Public User Methods                             */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getUserNames:                                                                */
    /* ---------------------------------------------------------------------------- */
    public ResultNameArray getUserNames()
     throws TapisClientException
    {
        // Make the REST call.
        RespNameArray resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.getUserNames(false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getUserNames:                                                                */
    /* ---------------------------------------------------------------------------- */
    public ResultNameArray getUserRoles(String user)
     throws TapisClientException
    {
        // Make the REST call.
        RespNameArray resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.getUserRoles(user, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getUserNames:                                                                */
    /* ---------------------------------------------------------------------------- */
    public ResultNameArray getUserPerms(String user)
     throws TapisClientException
    {
        // Make the REST call.
        RespNameArray resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.getUserPerms(user, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* grantUserRole:                                                               */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount grantUserRole(String user, String roleName)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqGrantUserRole();
        body.setUser(user);
        body.setRoleName(roleName);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.grantRole(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* removeUserRole:                                                              */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount removeUserRole(String user, String roleName)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqRemoveUserRole();
        body.setUser(user);
        body.setRoleName(roleName);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.removeRole(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* grantRoleWithPermission:                                                     */
    /* ---------------------------------------------------------------------------- */
    public ResultChangeCount grantRoleWithPermission(String user, String roleName,
                                                     String permSpec)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqGrantUserRoleWithPermission();
        body.setUser(user);
        body.setRoleName(roleName);
        body.setPermSpec(permSpec);
        
        // Make the REST call.
        RespChangeCount resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.grantRoleWithPermission(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* hasRole:                                                                     */
    /* ---------------------------------------------------------------------------- */
    public ResultAuthorized hasRole(String user, String roleName)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUserHasRole();
        body.setUser(user);
        body.setRoleName(roleName);
        
        // Make the REST call.
        RespAuthorized resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.hasRole(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* hasAnyRole:                                                                  */
    /* ---------------------------------------------------------------------------- */
    public ResultAuthorized hasRoleAny(String user, String[] roleNames)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUserHasRoleMulti();
        body.setUser(user);
        body.setRoleNames(Arrays.asList(roleNames));
        
        // Make the REST call.
        RespAuthorized resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.hasRoleAny(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* hasAllRole:                                                                  */
    /* ---------------------------------------------------------------------------- */
    public ResultAuthorized hasRoleAll(String user, String[] roleNames)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUserHasRoleMulti();
        body.setUser(user);
        body.setRoleNames(Arrays.asList(roleNames));
        
        // Make the REST call.
        RespAuthorized resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.hasRoleAll(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isPermitted:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public ResultAuthorized isPermitted(String user, String permSpec)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUserIsPermitted();
        body.setUser(user);
        body.setPermSpec(permSpec);
        
        // Make the REST call.
        RespAuthorized resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.isPermitted(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isPermittedAny:                                                              */
    /* ---------------------------------------------------------------------------- */
    public ResultAuthorized isPermittedAny(String user, String[] permSpecs)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUserIsPermittedMulti();
        body.setUser(user);
        body.setPermSpecs(Arrays.asList(permSpecs));
        
        // Make the REST call.
        RespAuthorized resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.isPermittedAny(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isPermittedAll:                                                              */
    /* ---------------------------------------------------------------------------- */
    public ResultAuthorized isPermittedAll(String user, String[] permSpecs)
     throws TapisClientException
    {
        // Assign input body.
        var body = new ReqUserIsPermittedMulti();
        body.setUser(user);
        body.setPermSpecs(Arrays.asList(permSpecs));
        
        // Make the REST call.
        RespAuthorized resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.isPermittedAll(body, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getUsersWithRole:                                                            */
    /* ---------------------------------------------------------------------------- */
    public ResultNameArray getUsersWithRole(String roleName)
     throws TapisClientException
    {
        // Make the REST call.
        RespNameArray resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.getUsersWithRole(roleName, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getUsersWithPermission:                                                      */
    /* ---------------------------------------------------------------------------- */
    public ResultNameArray getUsersWithPermission(String permSpec)
     throws TapisClientException
    {
        // Make the REST call.
        RespNameArray resp = null;
        try {
            // Get the API object using default networking.
            var userApi = new UserApi();
            resp = userApi.getUsersWithPermission(permSpec, false);
        }
        catch (Exception e) {throwTapisClientException(e);}
        
        // Return result value.
        return resp.getResult();
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* throwTapisClientException:                                                   */
    /* ---------------------------------------------------------------------------- */
    private void throwTapisClientException(Exception e)
     throws TapisClientException
    {
        // Initialize fields to be assigned to tapis exception.
        TapisResponse tapisResponse = null;
        int code = 0;
        String msg = null;
        
        // This should always be true.
        if (e instanceof ApiException) {
            // Extract information from the thrown exception.
            var apiException = (ApiException) e;
            String respBody = apiException.getResponseBody();
            if (respBody != null) 
                tapisResponse = _gson.fromJson(respBody, TapisResponse.class);
            code = apiException.getCode();
            msg  = e.getMessage();
        }

        // Use the extracted information if there's any.
        if (StringUtils.isBlank(msg))
            if (tapisResponse != null) msg = tapisResponse.message;
              else msg = EMsg.ERROR_STATUS.name();
        
        // Create the client exception.
        var clientException = new TapisClientException(msg, e);
        
        // Fill in as many of the tapis exception fields as possible.
        clientException.setCode(code);
        if (tapisResponse != null) {
            clientException.setStatus(tapisResponse.status);
            clientException.setTapisMessage(tapisResponse.message);
            clientException.setVersion(tapisResponse.version);
            clientException.setResult(tapisResponse.result);
        }
        
        // Throw the client exception.
        throw clientException;
    }
    
    /* **************************************************************************** */
    /*                                TapisResponse                                 */
    /* **************************************************************************** */
    // Data transfer class to hold generic response content temporarily.
    private static final class TapisResponse
    {
        private String status;
        private String message;
        private String version;
        private Object result;
    }
}

package edu.utexas.tacc.tapis.security.client;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;

import edu.utexas.tacc.tapis.security.client.gen.ApiException;
import edu.utexas.tacc.tapis.security.client.gen.api.RoleApi;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqCreateRole;
import edu.utexas.tacc.tapis.security.client.gen.model.RespChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.RespResourceUrl;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultChangeCount;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultResourceUrl;
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
    public SKClient(URI uri) {}
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* createRole:                                                                  */
    /* ---------------------------------------------------------------------------- */
    ResultResourceUrl createRole(String roleName, String description)
     throws TapisClientException
    {
        // Assign input body.
        ReqCreateRole body = new ReqCreateRole();
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
        
        // Check the response and return result if ok.
        checkResult(resp);
        return resp.getResult();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* deleteRoleByName:                                                            */
    /* ---------------------------------------------------------------------------- */
    ResultChangeCount deleteRoleByName(String roleName)
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
        
        // Check the response and return result if ok.
        checkResult(resp);
        return resp.getResult();
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* checkResult:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private void checkResult(Object resp) throws TapisClientException
    {
        // We should always get a non-null response.
        if (resp == null) throw new TapisClientException(EMsg.NO_RESPONSE.name());
        
        // This approach converts the response object that always has the tapis
        // response wrapper fields into json and then into our data transfer type.
        // This code should suffice even when new responses are added since the
        // four top-level fields will always be there.
        String respJson = _gson.toJson(resp);
        var tapisResp = _gson.fromJson(respJson, TapisResponse.class);
        
        // Throw exception on errors reported by the service.
        if (!STATUS_SUCCESS.equals(tapisResp.status)) {
            // Set the error message.
            String msg = tapisResp.message;
            if (StringUtils.isBlank(msg)) msg = EMsg.ERROR_STATUS.name();
            
            // Create and throw the client exception.
            var tapisClientException =  new TapisClientException(msg);
            tapisClientException.setTapisMessage(tapisResp.message);
            tapisClientException.setStatus(tapisResp.status);
            tapisClientException.setVersion(tapisResp.version);
            tapisClientException.setResult(tapisResp.result);
            throw tapisClientException;
        }
    }

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

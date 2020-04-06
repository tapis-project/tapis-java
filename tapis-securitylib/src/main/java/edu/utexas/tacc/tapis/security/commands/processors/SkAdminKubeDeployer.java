package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminResults;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1DeleteOptionsBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.Config;

public final class SkAdminKubeDeployer 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminKubeDeployer.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Fields passed to constructor.
    private final SkAdminParameters _parms;
    private final HashMap<String,HashMap<String,String>> _kubeSecretMap;
    
    // Kube API object.
    private CoreV1Api _coreApi;
    
    // Create the singleton instance for all processors to use.
    protected static final SkAdminResults _results = SkAdminResults.getInstance();
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminKubeDeployer(HashMap<String,HashMap<String,String>> kubeSecretMap,
                               SkAdminParameters parms)
    {
        _kubeSecretMap = kubeSecretMap;
        _parms = parms;
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    public void deploy()
    {
        // Maybe there's nothing to do.
        if (_kubeSecretMap.isEmpty()) return;
        
        // Double check for safety.
        if (!_parms.deployMerge && !_parms.deployReplace) return;
        
        // Connect to kubernetes.
        if (!connect()) return;
        
        // Perform merge operation if one is requested.
        if (_parms.deployMerge) mergeExistingSecrets();
        
        // Always remove existing secrets.
        removeExistingSecrets();
        
        // Write each secret to kubernetes.
        writeSecrets();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* connect:                                                               */
    /* ---------------------------------------------------------------------- */
    private boolean connect()
    {
        // Get a kube client and initialize api object.
        try {
            ApiClient apiClient = Config.fromToken(_parms.kubeUrl, _parms.kubeToken, 
                                                   _parms.kubeValidateSSL);
            _coreApi = new CoreV1Api(apiClient);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_ADMIN_KUBE_CONNECT_ERROR", _parms.kubeUrl, 
                                         _parms.kubeTokenEnv, _parms.kubeValidateSSL);
            _log.error(msg, e);
            
            // Cancel deployment.
            failAll(msg);
            return false;
        }
        
        // Connected.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* mergeExistingSecrets:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Merge each secret's existing key/value pairs into the secret's new map
     * for all keys that don't exist in the new map.  This allows the new map's
     * explicitly assigned values to take precedence over the existing values.  
     */
    private void mergeExistingSecrets()
    {
        // List of failed merges that must not be processed beyond this method.
        var failedList = new ArrayList<String>();
        
        // Try to merge the values from each existing secret.
        for (var entry : _kubeSecretMap.entrySet()) 
        {
            // ------- Read the current secret.
            V1Secret secret = null;
            try {
                secret = _coreApi.readNamespacedSecret(entry.getKey(), _parms.kubeNS, 
                                                       null, null, null);
            }
            catch (ApiException e) {
                // It's ok if there's nothing to merge.
                if (e.getCode() == 404) continue;
                
                // Some other API problem occurred. Record the failure and stage
                // the secret for removal from the deployment map.
                String msg = MsgUtils.getMsg("SK_ADMIN_KUBE_READ_SECRET", entry.getKey(),
                                             _parms.kubeNS, "(http "+ e.getCode() + ") " + e.getMessage());
                _results.recordDeployFailure(entry.getValue().size(),
                    makeFailureMessage(entry.getKey(), entry.getValue().size(), msg));
                failedList.add(entry.getKey());
                continue;
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ADMIN_KUBE_READ_SECRET", entry.getKey(),
                                             _parms.kubeNS, e.getMessage());
                _results.recordDeployFailure(entry.getValue().size(),
                    makeFailureMessage(entry.getKey(), entry.getValue().size(), msg));
                failedList.add(entry.getKey());
                continue;
            }
            
            // ------- Merge non-conflicting key/values.
            // Get the existing secret's key/value pairs.
            Map<String,byte[]> existingMap = secret.getData();
            if (existingMap == null || existingMap.isEmpty()) continue;
            
            // Preserve all key/value pairs that are not in the existing map 
            // by adding them to the new map.  The existingMap's values are 
            // byte arrays encoded in base64, but we put them into newMap as 
            // strings with the encoding unchanged. 
            Map<String,String> newMap = entry.getValue();
            for (var existingEntry : existingMap.entrySet()) {
                if (newMap.containsKey(existingEntry.getKey())) continue;
                newMap.put(existingEntry.getKey(), new String(existingEntry.getValue()));
            }
        }
        
        // ------- Remove any failed merges from deployment.
        for (var secretName : failedList) _kubeSecretMap.remove(secretName);
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeExistingSecrets:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Remove all secrets present in the new map from Kubernetes.  When a merge
     * deployment is specified, all of a secret's existing key/value pairs that 
     * need to be preserved have already been inserted in the secret's new map.
     */
    private void removeExistingSecrets()
    {
        // Create the delete options.  Setting the kind field breaks things.
        V1DeleteOptions opts = new V1DeleteOptionsBuilder().
                               withApiVersion("v1").
                               withGracePeriodSeconds(0L).
                               withPropagationPolicy("Foreground").
                               build();
        
        // Remove each secret that currently exists if possible.
        // This is a best effort deal, we ignore all failures 
        // especially secret not found.
        for (var entry : _kubeSecretMap.entrySet()) 
        {
            V1Status v1Status = null;
            try {
                v1Status = _coreApi.deleteNamespacedSecret(entry.getKey(), _parms.kubeNS, 
                                                null, null, 0, null, "Foreground", opts);
            }
            catch (Exception e) {} 
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* writeSecrets:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Write each secret to kubernetes.  All writes should succeed without 
     * conflict since all of the ssecrets in the new map have been deleted from
     * kubernetes.
     */
    private void writeSecrets()
    {
        for (var entry : _kubeSecretMap.entrySet()) 
        {
            // Create the metadata.
            V1ObjectMeta meta = new V1ObjectMeta();
            meta.setName(entry.getKey());
            meta.setNamespace(_parms.kubeNS);
            
            // The map values are required to already be base64 encoded, 
            // but we now need them to be presented in byte arrays.
            var entryMap = entry.getValue();
            HashMap<String,byte[]> dataMap = new HashMap<>(1 + entryMap.size() * 2);
            for (var kvEntry : entryMap.entrySet()) 
                dataMap.put(kvEntry.getKey(), kvEntry.getValue().getBytes());
            
            // Create a secret.  Opaque is the default type.
            V1Secret secret = new V1Secret();
            secret.setApiVersion("v1");
            secret.setKind("Secret");
            secret.setData(dataMap);
            secret.setMetadata(meta);

            // Write the secret.
            V1Secret newSecret = null;
            try {
                newSecret = _coreApi.createNamespacedSecret(_parms.kubeNS, secret, 
                                                            null, null,  null);
            }
            catch (ApiException e) {
                String msg = MsgUtils.getMsg("SK_ADMIN_KUBE_WRITE_SECRET", entry.getKey(),
                              _parms.kubeNS, "(http "+ e.getCode() + ") " + e.getMessage());
                _results.recordDeployFailure(entry.getValue().size(),
                    makeFailureMessage(entry.getKey(), entry.getValue().size(), msg));
                continue;
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_ADMIN_KUBE_WRITE_SECRET", entry.getKey(),
                                             _parms.kubeNS, e.getMessage());
                _results.recordDeployFailure(entry.getValue().size(),
                    makeFailureMessage(entry.getKey(), entry.getValue().size(), msg));
                continue;
            }
            
            // Success.
            int keyCount = dataMap.size();
            _results.recordDeploySuccess(keyCount, 
                                         makeSuccessMessage(entry.getKey(), keyCount));
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* failAll:                                                               */
    /* ---------------------------------------------------------------------- */
    private void failAll(String msg)
    {
        for (var entry : _kubeSecretMap.entrySet()) 
            _results.recordDeployFailure(entry.getValue().size(),
                      makeFailureMessage(entry.getKey(), entry.getValue().size(), msg));
    }

    /* ---------------------------------------------------------------------- */
    /* makeFailureMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeFailureMessage(String secretName, int keyCount, String errorMsg)
    {
        // Set the failed flag to alert any subsequent processing.
        return " FAILED to deploy secret \"" + secretName + "\" with " + keyCount +
               " key(s) to Kubernetes: "  + errorMsg;
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSuccessMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSuccessMessage(String secretName, int keyCount)
    {
        return " SUCCESSFUL deployment of secret \"" + secretName + "\" with " + 
               keyCount + " key(s) to Kubernetes.";
    }
}

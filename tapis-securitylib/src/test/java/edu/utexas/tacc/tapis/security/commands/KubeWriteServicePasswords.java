package edu.utexas.tacc.tapis.security.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.util.Config;

public class KubeWriteServicePasswords 
{
    // ----- Constants.
    // The key for the service password value.
    private static final String SERVICE_PWD_KEY = "service-password";
    
    private static final LinkedHashMap<String,String> _secretsMap;
    
    static {
        // Fill in the actual passwords here, but don't save them to the repo.
        _secretsMap = new LinkedHashMap<>(23);
        _secretsMap.put("tapis-apps-secrets",           "xxx");
        _secretsMap.put("tapis-authenticator-secrets",  "xxx");
        _secretsMap.put("tapis-files-secrets",          "xxx");
        _secretsMap.put("tapis-jobs-secrets",           "xxx");
        _secretsMap.put("tapis-meta-secrets",           "xxx");
        _secretsMap.put("tapis-streams-secrets",        "xxx");
        _secretsMap.put("tapis-systems-secrets",        "xxx");
        _secretsMap.put("tapis-tenants-secrets",        "xxx");
    }
    
    // ----- Fields.
    private CoreV1Api _coreApi;
    private String    _kubeToken;
    private String    _kubeUrl;
    private String    _kubeNS;
    
    private int           _success;
    private List<String>  _failedList = new ArrayList<String>();
    
    
    
    
    public static void main(String[] args) throws ApiException, IOException 
    {
        var obj = new KubeWriteServicePasswords();
        obj.execute();
        
    }

    private void execute()
    {
        // Get the environment variables.
        _kubeToken = System.getenv("KUBE_TOKEN");
        _kubeUrl   = System.getenv("KUBE_URL");
        _kubeNS    = System.getenv("KUBE_NAMESPACE"); 
        
        // Get a kube client.
        ApiClient apiClient = Config.fromToken(_kubeUrl, _kubeToken, false);
        _coreApi = new CoreV1Api(apiClient);
        
        // Create each secret.
        for (var entry : _secretsMap.entrySet()) createSecret(entry);
        
        // Write summary.
        System.out.println();
        System.out.println("Secrets created: " + _success);
        System.out.println("Secrets failed : " + _failedList.size());
        if (!_failedList.isEmpty()) {
            System.out.println("  Failed secrets:");
            for (var secretName : _failedList)  System.out.println("    " + secretName);
        }
    }
    
    private void createSecret(Entry<String,String> entry)
    {
        // Get the key/value map.
        var map = new HashMap<String,String>();
        map.put(SERVICE_PWD_KEY, entry.getValue());
        
        // Create the metadata.
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(entry.getKey());
        meta.setNamespace(_kubeNS);
      
        // Create a secret.  Opaque is the default type.
        V1Secret secret = new V1Secret();
        secret.setApiVersion("v1");
        secret.setKind("Secret");
        secret.setStringData(map);
        secret.setMetadata(meta);
      
        // Make the call.
        V1Secret newSecret = null;
        try {newSecret = _coreApi.createNamespacedSecret(_kubeNS, secret, null, null,  null);}
            catch (ApiException e) {
                _failedList.add(entry.getKey() + " (" + e.getCode() + "): " + e.getMessage());
                return;
            }
            catch (Exception e) {
                _failedList.add(entry.getKey() + ": " + e.getMessage());
                return;
            }
        
        // Increment success count.
        _success++;
      
        // Make some noise.
        System.out.println("---------------------");
        System.out.println(newSecret);
        System.out.println("---------------------");
    }
}

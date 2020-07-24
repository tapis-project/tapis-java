package edu.utexas.tacc.tapis.security.commands;

import java.io.IOException;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.util.Config;

public class KubeReadSecrets 
{
    private static final String SECRET_NAME = "rich-test-secret";
    
    public static void main(String[] args) throws ApiException, IOException 
    {
        // Get the required kubernetes information from the environment.
        String kubeToken = System.getenv("KUBE_TOKEN");
        String kubeUrl   = System.getenv("KUBE_URL");
        String kubeNS    = System.getenv("KUBE_NAMESPACE"); 
        
        // Get a kube client.
        ApiClient apiClient = Config.fromToken(kubeUrl, kubeToken, false);
        // ApiClient apiClient = Config.fromConfig("/home/rcardone/.kube/config");
        CoreV1Api coreApi = new CoreV1Api(apiClient);
        
        // Make the call.
        V1Secret secret = null;
        try {secret = coreApi.readNamespacedSecret(SECRET_NAME, kubeNS, null, null,  null);}
            catch (ApiException e) {
                e.printStackTrace();
                return;
            }
            catch (Exception e) {
                e.printStackTrace();
                return;
            }
        
        System.out.println(secret);
    }

}

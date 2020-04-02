package edu.utexas.tacc.tapis.security.commands;

import java.io.IOException;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1DeleteOptionsBuilder;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.Config;

public class KubeDeleteSecrets 
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
        
        // Create the delete options.  Setting the kind here is will not work!
        V1DeleteOptions opts = new V1DeleteOptionsBuilder().
                               withApiVersion("v1").
                             //  withKind("Secret").
                               withGracePeriodSeconds(0L).
                               withPropagationPolicy("Foreground").
                               build();
        
        // Make the call.
        V1Status v1Status = null;
        try {v1Status = coreApi.deleteNamespacedSecret(SECRET_NAME, kubeNS, null, null, 0, null, "Foreground", opts);}
            catch (ApiException e) {
                e.printStackTrace();
                return;
            }
            catch (Exception e) {
                e.printStackTrace();
                return;
            }
        
        System.out.println(v1Status);
    }

}

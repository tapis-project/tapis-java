package edu.utexas.tacc.tapis.security.commands;

import java.io.IOException;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;

public class KubeConnect 
{
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
        
        // Do some read-only command.
        V1PodList podList = coreApi.listNamespacedPod(kubeNS,  null, true, null, null, null, null, null, null, null, null);
        if (podList == null) {
            System.out.println("Null pod list returned.");
            return;
        }
        
        // Print results.
        for (V1Pod pod : podList.getItems()) {
            System.out.println(pod);
        }
    }

}

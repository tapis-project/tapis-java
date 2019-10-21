package edu.utexas.tacc.tapis.systems.client;

import edu.utexas.tacc.tapis.systems_client.ApiClient;
import edu.utexas.tacc.tapis.systems_client.ApiResponse;
import edu.utexas.tacc.tapis.systems_client.api.SystemsApi;

public class ClientTestMain
{
  public static void main(String[] args) throws Exception
  {
    System.out.println("Starting Systems ClientTest");
    ApiClient  apiClient = new ApiClient();
    apiClient.setBasePath("http://localhost:8080/systems/v3");
    apiClient.setDebugging(true);
    SystemsApi sysApi = new SystemsApi(apiClient);
    ApiResponse resp = sysApi.getSystemByNameWithHttpInfo("system1", Boolean.TRUE, Boolean.FALSE);
    System.out.println(resp);
  }
}

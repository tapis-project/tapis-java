package edu.utexas.tacc.tapis.systems.client;

//import edu.utexas.tacc.tapis.systems_client.ApiClient;
//import edu.utexas.tacc.tapis.systems_client.ApiResponse;
//import edu.utexas.tacc.tapis.systems_client.api.SystemsApi;
//import edu.utexas.tacc.tapis.systems_client.model.CreateSystem;
//import edu.utexas.tacc.tapis.systems_client.model.Protocol;

public class ClientTestMain
{
  public static void main(String[] args) throws Exception
  {
//    System.out.println("Starting Systems ClientTest");
//    ApiClient  apiClient = new ApiClient();
//    apiClient.setBasePath("http://localhost:8080/systems/v3");
//    apiClient.setDebugging(true);
//    SystemsApi sysApi = new SystemsApi(apiClient);
//
//    // Create Protocol component needed for system create request
//    Protocol reqProtocol = new Protocol();
//    reqProtocol.setAccessMechanism("SSH_PASSWORD");
//    reqProtocol.setPort(22);
//    reqProtocol.setUseProxy(false);
//    reqProtocol.setProxyHost("");
//    reqProtocol.setProxyPort(1111);
//
//    // Create system request
//    String name = "system1";
//    CreateSystem reqCreateSystem = new CreateSystem();
//    reqCreateSystem.setName(name);
//    reqCreateSystem.setDescription("Description of " + name);
//    reqCreateSystem.setOwner("jdoe");
//    reqCreateSystem.setAvailable(true);
//    reqCreateSystem.name(name).setHost("data.tacc.utexas.edu");
//    reqCreateSystem.setRootDir("/${tenant}/home");
//    reqCreateSystem.setWorkDir("/home/${apiUserId}/tapis/data");
//    reqCreateSystem.setScratchDir("");
//    reqCreateSystem.setJobInputDir("");
//    reqCreateSystem.setJobOutputDir("");
//    reqCreateSystem.setEffectiveUserId("");
//    reqCreateSystem.setAccessCredential("");
//    reqCreateSystem.setProtocol(reqProtocol);
//
//    // Create a system
//    System.out.println("Creating system with name: " + name);
//    ApiResponse resp = sysApi.createSystemWithHttpInfo(reqCreateSystem, true);
//    System.out.println(resp);
//
//    // Retrieve the system
//    System.out.println("Retrieving system with name: " + name);
//    resp = sysApi.getSystemByNameWithHttpInfo(name, Boolean.TRUE, Boolean.FALSE);
//    System.out.println(resp);
//
//    // TODO Delete the system
//    System.out.println("Deleting system with name: " + name);
////    resp = sysApi.getSystemByNameWithHttpInfo(name, Boolean.TRUE);
  }
}

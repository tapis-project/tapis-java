package edu.utexas.tacc.tapis.systems.client;

//import edu.utexas.tacc.tapis.systems_client.ApiClient;
//import edu.utexas.tacc.tapis.systems_client.ApiResponse;
//import edu.utexas.tacc.tapis.systems_client.api.SystemsApi;
//import edu.utexas.tacc.tapis.systems_client.model.Protocol;
//import edu.utexas.tacc.tapis.systems_client.model.ReqCreateSystem;

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
//    Protocol protocol = new Protocol();
//    protocol.setAccessMechanism("SSH_PASSWORD");
//    protocol.setPort(22);
//    protocol.setUseProxy(false);
//    protocol.setProxyHost("");
//    protocol.setProxyPort(1111);
//
//    // Create system request
//    String name = "systemA";
//    ReqCreateSystem reqCreateSystem = new ReqCreateSystem();
//    reqCreateSystem.setName(name);
//    reqCreateSystem.setDescription("Description of " + name);
//    reqCreateSystem.setOwner("jdoe");
//    reqCreateSystem.setAvailable(true);
//    reqCreateSystem.name(name).setHost("data.tacc.utexas.edu");
//    reqCreateSystem.setRootDir("/${tenant}/home");
//    reqCreateSystem.setJobInputDir("/home/${apiUserId}/input");
//    reqCreateSystem.setJobOutputDir("/home/${apiUserId}/output");
//    reqCreateSystem.setWorkDir("/home/${apiUserId}/tapis/data");
//    reqCreateSystem.setScratchDir("/scratch/${tenant}/${apiUserId}");
//    reqCreateSystem.setEffectiveUserId("${apiUserId}");
//    reqCreateSystem.setAccessCredential("<password>");
//    reqCreateSystem.setProtocol(protocol);
//
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
////    resp = sysApi.delete
  }
}

//package edu.utexas.tacc.tapis.systems.client;
//
//import edu.utexas.tacc.tapis.systems.client.gen.ApiClient;
//import edu.utexas.tacc.tapis.systems.client.gen.api.SystemsApi;
//import edu.utexas.tacc.tapis.systems.client.gen.model.*;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
//
//public class ClientTestMain
//{
//  public static void main(String[] args) throws Exception
//  {
//    System.out.println("Starting Systems ClientTest");
//    ApiClient  apiClient = new ApiClient();
////    apiClient.setBasePath("http://localhost:8080/systems/v3");
//    apiClient.setBasePath("http://c002.rodeo.tacc.utexas.edu:32656/systems/v3");
//    apiClient.setDebugging(true);
//    apiClient.addDefaultHeader("X-Tapis-Test-Tenant", "tenant1");
//    apiClient.addDefaultHeader("X-Tapis-Test-User", "tapisuser1");
//    apiClient.addDefaultHeader("X-Tapis-Test-Account-Type", "user");
//    SystemsApi sysApi = new SystemsApi(apiClient);
//
//    // Create Protocol component needed for system create request
////    List<TSystem.TransferMechanismsEnum> transferMechs = new ArrayList<>(List.of(TSystem.TransferMechanismsEnum.S3));
////    List<String> transferMechs = Collections.emptyList();
//    List<String> transferMechs = new ArrayList<>(List.of(TSystem.TransferMechanismsEnum.S3.name()));
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
//    reqCreateSystem.setAccessCredential("fakePassword1");
//    reqCreateSystem.setAccessMechanism("SSH_PASSWORD");
//    reqCreateSystem.setTransferMechanisms(transferMechs);
//    reqCreateSystem.setPort(22);
//    reqCreateSystem.setUseProxy(false);
//    reqCreateSystem.setProxyHost("");
//    reqCreateSystem.setProxyPort(1111);
//
//
//    // Create a system
//    System.out.println("Creating system with name: " + name);
//    RespResourceUrl respUrl = sysApi.createSystem(reqCreateSystem, true);
//    System.out.println("Created system: " + respUrl.getResult().getUrl());
//
//    // Retrieve the system
//    System.out.println("Retrieving system with name: " + name);
//    RespSystem respSystem = sysApi.getSystemByName(name, Boolean.TRUE, Boolean.FALSE);
//    System.out.println("Retrieved system: " + respSystem.getResult());
//
//    // Delete the system
//    System.out.println("Deleting system with name: " + name);
//    RespChangeCount respChangeCount = sysApi.deleteSystemByName(name, true);
//    System.out.println("Number of items deleted: " + respChangeCount.getResult().getChanges());
//  }
//}

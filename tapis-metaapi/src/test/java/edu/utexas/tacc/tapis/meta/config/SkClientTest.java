package edu.utexas.tacc.tapis.meta.config;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;

import java.util.List;


public class SkClientTest {
  public static void main(String[] args) throws TapisClientException
  {
    String url = "https://dev.develop.tapis.io/v3";
    String jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI4YmY4MjRmOC00ZGFlLTQ4MjEtYTlkMS1lYjczOTU5ZWFjM2UiLCJpc3MiOiJodHRwczovL21hc3Rlci5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRhY2NNZXRhQWRtaW5AbWFzdGVyIiwidGFwaXMvdGVuYW50X2lkIjoibWFzdGVyIiwidGFwaXMvdG9rZW5fdHlwZSI6ImFjY2VzcyIsInRhcGlzL2RlbGVnYXRpb24iOmZhbHNlLCJ0YXBpcy9kZWxlZ2F0aW9uX3N1YiI6bnVsbCwidGFwaXMvdXNlcm5hbWUiOiJ0YWNjTWV0YUFkbWluIiwidGFwaXMvYWNjb3VudF90eXBlIjoic2VydmljZSIsImV4cCI6MTYxMzE4NTIyNn0.rsggdOQV-QuU4mgk1idHHhyI8-cR_5yML571Lt-osbRlinrrapEGoAkCQNqzL4k-rg2aI1VSx2cgpSVg2ZIB-D0k7nCQTMNEJNEbMcYC5AUU1RH5KoDKEZbuRkSEkfNPvClcB-TZIULrGdiv3yrERdDw64qzjGJQTdpXIN3YXTya4uZzU1XKKM3xMassh5jQo0r3jbKUf7eOE4ZPPAod88Qh7bGZdWCmyPEbaiOTgZ-15DZ4dpebjRG-qSWWLO0u91JoN-f3_rF1dvtuTCqSr4YLjQcliDLPVDePDiaDxqcBRwgohPU9M5RwUuMuD6dsQFeRt8aAW9lzHgCMcgyd9g";
    
    SKClient skClient = new SKClient(url,jwt);
    // Service to Service calls require user header, set it to be the same as the service name
    // TODO Get string constants from shared code when available
    String TAPIS_USER_HEADER = "X-Tapis-User";
    String TAPIS_TENANT_HEADER = "X-Tapis-Tenant";
    
    skClient.addDefaultHeader(TAPIS_USER_HEADER, "streams");
    // todo this will change based on header processing so just hardwire for now
    skClient.addDefaultHeader(TAPIS_TENANT_HEADER, "master");
    Boolean isAuthorized = skClient.isPermitted("dev","streams", "meta:dev:GET,POST,PUT:StreamsTACCDB:*:*");
    // List roles = skClient.getUserNames();
    System.out.println(isAuthorized);
    
    
    
    
    
  }
}

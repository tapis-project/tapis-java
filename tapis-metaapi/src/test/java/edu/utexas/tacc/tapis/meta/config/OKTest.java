package edu.utexas.tacc.tapis.meta.config;

import okhttp3.*;

import java.io.IOException;

public class OKTest {
  //-----------------------------------
  //           main
  //-----------------------------------
  
  public static void main(String[] args) {
    // create a factory to build clients from single instance.
    System.out.println();
    OkHttpClient okHttpClient = OkSingleton.getInstance();
  
    Request request = new Request.Builder()
        .url("http://c002.rodeo.tacc.utexas.edu:30401/")
        .build();
    
    try(Response response = okHttpClient.newCall(request).execute()){
      System.out.println(response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  
    MediaType JSON = MediaType.get("application/json; charset=utf-8");
    String url = "https://dev.develop.tapis.io/v3/security/user/isPermitted";
    String jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI4YmY4MjRmOC00ZGFlLTQ4MjEtYTlkMS1lYjczOTU5ZWFjM2UiLCJpc3MiOiJodHRwczovL21hc3Rlci5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRhY2NNZXRhQWRtaW5AbWFzdGVyIiwidGFwaXMvdGVuYW50X2lkIjoibWFzdGVyIiwidGFwaXMvdG9rZW5fdHlwZSI6ImFjY2VzcyIsInRhcGlzL2RlbGVnYXRpb24iOmZhbHNlLCJ0YXBpcy9kZWxlZ2F0aW9uX3N1YiI6bnVsbCwidGFwaXMvdXNlcm5hbWUiOiJ0YWNjTWV0YUFkbWluIiwidGFwaXMvYWNjb3VudF90eXBlIjoic2VydmljZSIsImV4cCI6MTYxMzE4NTIyNn0.rsggdOQV-QuU4mgk1idHHhyI8-cR_5yML571Lt-osbRlinrrapEGoAkCQNqzL4k-rg2aI1VSx2cgpSVg2ZIB-D0k7nCQTMNEJNEbMcYC5AUU1RH5KoDKEZbuRkSEkfNPvClcB-TZIULrGdiv3yrERdDw64qzjGJQTdpXIN3YXTya4uZzU1XKKM3xMassh5jQo0r3jbKUf7eOE4ZPPAod88Qh7bGZdWCmyPEbaiOTgZ-15DZ4dpebjRG-qSWWLO0u91JoN-f3_rF1dvtuTCqSr4YLjQcliDLPVDePDiaDxqcBRwgohPU9M5RwUuMuD6dsQFeRt8aAW9lzHgCMcgyd9g";
    
    String json = "{\"tenant\":\"master\",\"user\":\"streams\",\"permSpec\":\"meta:master:GET,POST,PUT:StreamsTACCDB:*:*\"}";
    
    RequestBody body = RequestBody.create(json, JSON);
    Request request1 = new Request.Builder()
        .url(url)
        .addHeader("X-Tapis-User","streams")
        .addHeader("X-Tapis-Tenant","master")
        .addHeader("X-Tapis-Token",jwt)
        .post(body)
        .build();
  
    try (Response response = okHttpClient.newCall(request1).execute()) {
      System.out.println(response.body().string());
    } catch (IOException e) {
      e.printStackTrace();
    }
  
    // results of call same okhttp client with different urls
    //["StreamsTACCDB","testdb","v1airr"]
    //{"result":{"isAuthorized":true},"status":"success","message":"TAPIS_AUTHORIZED User authorized: streams authorized: true","version":"0.0.1"}
  
  }
}

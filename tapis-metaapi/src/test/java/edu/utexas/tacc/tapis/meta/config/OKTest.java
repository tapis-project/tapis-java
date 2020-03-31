package edu.utexas.tacc.tapis.meta.config;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

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

    
  }
}

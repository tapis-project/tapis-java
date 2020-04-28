package edu.utexas.tacc.tapis.meta.integration;

import io.restassured.RestAssured;
import org.hamcrest.Matchers;
import io.restassured.matcher.RestAssuredMatchers;
import org.junit.BeforeClass;
import org.junit.Test;

public class RootIntegrationTest {
  @BeforeClass
  public static void before(){
    System.out.println("Setting up ....");
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "/v3/meta/";
    RestAssured.port = 8080;

  }
  
  @Test
  public void sanityTest(){
   
   RestAssured.given()
              .header("X-Tapis-Token","eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6IlN0cmVhbXNBZG1pbkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6IlN0cmVhbXNBZG1pbiIsInRhcGlzL2FjY291bnRfdHlwZSI6InNlcnZpY2UiLCJleHAiOjE1OTA5MjAwNTd9.FyxgS5AWo151VsQGMXhZYDj_QD9rpWG-iPqeRUl9mAazek5ExY9JfvMdYHYAQbzKl1dnx74qEy0JBP2qT_Fv8ErPZTDFGo8Cbfw93PrJBbcG63CjHvTEV5WK8OQ5Yh7FU35szf_icOXAHdOJ00RIiX1qEhCup-88LwgPCX2aKM_9VlWsjfebcKbBdtpZkjtshTNgJME5pTZFNbyH0dzl7dD-OuTZMiEYrWGaCeWenjX4Wui3fSh6JD4zq-4iQTqDrc-XNSKsEb76qUSu9Aph9whaylPUNAqPcw1N6n6j41gsPpGJQVXmdUGSCDUE976M3oEOlAQVW1Lj8Rdu3UVHdw")
              .header("X-Tapis-User","streamsTACCAdmin")
              .header("X-Tapis-Tenant","dev")
              .contentType("application/json")
              .get("StreamsTACCDB")
              .then()
              .statusCode(200).toString();
  }
}

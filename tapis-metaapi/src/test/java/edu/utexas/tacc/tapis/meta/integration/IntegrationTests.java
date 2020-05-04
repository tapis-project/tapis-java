package edu.utexas.tacc.tapis.meta.integration;

import io.restassured.RestAssured;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;

public class IntegrationTests {
  private static String token;
  private static String user;
  private static String tenant;
  private static RequestSpecification requestSpecification;
  private static long testrun = new DateTime().getMillis();
  
  protected String etagDB;
  protected String etagCollection;
  protected String documentId;
  
  
  @BeforeClass
  public static void before(){
    System.out.println("Setting up ....");
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "/v3/meta/";
    RestAssured.port = 8080;
    token = System.getenv("token");
    user = "streams";
    tenant = "master";
    requestSpecification =    RestAssured.given()
                                         .header("X-Tapis-Token", IntegrationTests.token)
                                         .header("X-Tapis-User", IntegrationTests.user)
                                         .header("X-Tapis-Tenant", IntegrationTests.tenant)
                                         .contentType("application/json");
  }
  
  @Test(enabled = false)
  public void sanityTest(){
   // meta:master token
    IntegrationTests.requestSpecification
              .get("StreamsTACCDB")
              .then()
              .statusCode(200).toString();
  }
  
  /**************************************************
   *    root endpoints
   **************************************************/
  @Test(enabled = false)
  public void rootTest(){
    // meta:master token
    IntegrationTests.requestSpecification
        .get("")
        .then()
        .statusCode(200).toString();
  }
  
  /**************************************************
   *    db endpoints
   **************************************************/
  
  @Test(enabled = true)
  public void listCollectionNames(){
    // meta:master token
    IntegrationTests.requestSpecification
        .get("StreamsTACCDB")
        .then()
        .statusCode(200).toString();
  }
  
  @Test(enabled = false)
  public void createDB(){
    IntegrationTests.requestSpecification
        .put("TestDB")
        .then()
        .statusCode(201).toString();
  }
  
  @Test(enabled = false)
  public void deleteDB(){
    IntegrationTests.requestSpecification
        .delete("TestDB")
        .then()
        .statusCode(204).toString();
  }
  
  @Test(enabled = true)
  public void getDBMetadata(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .get("StreamsTACCDB/_meta")
        .then()
        .statusCode(200).extract().response();
    this.etagDB = response.jsonPath().getString("_etag.$oid");
  }
  
  /**************************************************
   *    collection endpoints
   **************************************************/

  @Test(enabled = false)
  public void createCollection(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .put("StreamsTACCDB/TstCollection")
        .then()
        .statusCode(201).extract().response();
    // TODO etag from header
    // this.etagDB = response.jsonPath().getString("_etag.$oid");
  }
  
  @Test(enabled = true)
  public void listDocuments(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .get("StreamsTACCDB/TstCollection")
        .then()
        .statusCode(200).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = true)
  public void getCollectionSize(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .get("StreamsTACCDB/TstCollection/_size")
        .then()
        .statusCode(200).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = true)
  public void getCollectionMetadata(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .get("StreamsTACCDB/TstCollection/_meta")
        .then()
        .statusCode(200).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = true)
  public void createDocument(){
    RestAssured.defaultParser = Parser.JSON;
    String requestBody = "{ \"_id\":\"testdoc\",\"name\": \""+IntegrationTests.testrun+"\", \"jimmyList\":[\"1\",\"3\"],\"description\": \"new whatever\"}";
    Response response = IntegrationTests.requestSpecification
        .queryParam("basic", true)
        .body(requestBody)
        .post("StreamsTACCDB/TstCollection")
        .then()
        .statusCode(201).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = false)
  public void deleteCollection(){   // TODO add header if-match
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .delete("StreamsTACCDB/TstCollection")
        .then()
        .statusCode(204).extract().response();
    System.out.println(response.toString());
  }
  
  /**************************************************
   *    document endpoints
   **************************************************/

  @Test(enabled = true)
  public void getDocument(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .get("StreamsTACCDB/TstCollection/testdoc")
        .then()
        .statusCode(200).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = false)   //  TODO not in ResourceBucket
  public void getDocumentMetadata(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .get("StreamsTACCDB/TstCollection/testdoc/_meta")
        .then()
        .statusCode(200).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = true)
  public void replaceDocument(){
    RestAssured.defaultParser = Parser.JSON;
    String requestBody = "{ \"_id\":\"testdoc\",\"name\": \""+IntegrationTests.testrun+"\", \"jimmyList\":[\"1\",\"3\"],\"description\": \"replace document test\"}";
    Response response = IntegrationTests.requestSpecification
        .body(requestBody)
        .put("StreamsTACCDB/TstCollection/testdoc")
        .then()
        .statusCode(201).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = true)
  public void modifyDocument(){
    RestAssured.defaultParser = Parser.JSON;
    String requestBody = "{ \"description\": \"modify this document test\"}";
    Response response = IntegrationTests.requestSpecification
        .body(requestBody)
        .patch("StreamsTACCDB/TstCollection/testdoc")
        .then()
        .statusCode(201).extract().response();
    System.out.println(response.toString());
  }
  
  @Test(enabled = true)
  public void deleteDocument(){
    RestAssured.defaultParser = Parser.JSON;
    Response response = IntegrationTests.requestSpecification
        .delete("StreamsTACCDB/TstCollection/testdoc")
        .then()
        .statusCode(204).extract().response();
    System.out.println(response.toString());
  }
  
  
  
  
  
  
}

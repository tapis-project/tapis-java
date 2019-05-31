package edu.utexas.tacc.tapis.sharedapi.dto;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper;
import edu.utexas.tacc.tapis.sharedapi.dto.ResponseWrapper.RESPONSE_STATUS;

@Test(groups={"unit"})
public class ResponseWrapperTest 
{
  /* **************************************************************************** */
  /*                                    Tests                                     */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* testJobSubmitRequest1:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Test adding a result object to a response wrapper using 2 gson generators. */
  @Test(enabled=true)
  public void ResponseWrapperTest1()
  {
    // Get the wrapper tree.
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.success, "hi, this is a message");
    JsonObject obj = (JsonObject) TapisGsonUtils.getGson().toJsonTree(wrapper);
    
    // Get the test dto tree.
    TestDTO dto = new TestDTO();
    JsonObject dtoObj = (JsonObject) TapisGsonUtils.getGson().toJsonTree(dto);
    
    // Add the dto to the wrapper as we would in a REST response.
    obj.add("result", dtoObj);
    System.out.println(obj.toString());
    
    // Check for expected json string ignoring the tapis version number.
    Assert.assertTrue(obj.toString().startsWith("{\"status\":\"success\",\"message\":\"hi, this is a message\",\"version\":"), "Unexpected wrapped DTO start");
    Assert.assertTrue(obj.toString().endsWith(",\"result\":{\"string1\":\"xxx\",\"int1\":88,\"bool1\":true,\"map1\":{\"Four\":4,\"Two\":2},\"instanceArray\":[\"1970-01-01T00:00:00Z\",\"+1000000000-12-31T23:59:59.999999999Z\"]}}"), 
                      "Unexpected wrapped DTO end");
  }

  /* ---------------------------------------------------------------------------- */
  /* testJobSubmitRequest2:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Test adding a result object to a response wrapper using 1 gson generator. */
  @Test(enabled=true)
  public void ResponseWrapperTest2()
  {
    // Create a single generator.
    Gson gson = TapisGsonUtils.getGson();
    
    // Get the wrapper tree.
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.success, "hi, this is a message");
    JsonObject obj = (JsonObject) gson.toJsonTree(wrapper);
    
    // Get the test dto tree.
    TestDTO dto = new TestDTO();
    JsonObject dtoObj = (JsonObject) gson.toJsonTree(dto);
    
    // Add the dto to the wrapper as we would in a REST response.
    obj.add("result", dtoObj);
    System.out.println(obj.toString());
    
    // Check for expected json string ignoring the tapis version number.
    Assert.assertTrue(obj.toString().startsWith("{\"status\":\"success\",\"message\":\"hi, this is a message\",\"version\":"), "Unexpected wrapped DTO start");
    Assert.assertTrue(obj.toString().endsWith(",\"result\":{\"string1\":\"xxx\",\"int1\":88,\"bool1\":true,\"map1\":{\"Four\":4,\"Two\":2},\"instanceArray\":[\"1970-01-01T00:00:00Z\",\"+1000000000-12-31T23:59:59.999999999Z\"]}}"), 
                      "Unexpected wrapped DTO end");
  }

  /* ---------------------------------------------------------------------------- */
  /* testJobSubmitRequest3:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Test adding a result object to a response wrapper using the wrapper api and
   * pretty print turned off and a null message. */
  @Test(enabled=true)
  public void ResponseWrapperTest3()
  {
    // Get the completed wrapper as a string.
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.success, null);
    String json = wrapper.addResult(new TestDTO(), false);
    System.out.println(json);
    
    // Check for expected json string ignoring the tapis version number.
    Assert.assertTrue(json.startsWith("{\"status\":\"success\",\"message\":null,\"version\":"), "Unexpected wrapped DTO start");
    Assert.assertTrue(json.endsWith(",\"result\":{\"string1\":\"xxx\",\"int1\":88,\"bool1\":true,\"map1\":{\"Four\":4,\"Two\":2},\"instanceArray\":[\"1970-01-01T00:00:00Z\",\"+1000000000-12-31T23:59:59.999999999Z\"]}}"), 
                      "Unexpected wrapped DTO end");
  }
  
  /* ---------------------------------------------------------------------------- */
  /* testJobSubmitRequest4:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Test adding a result object to a response wrapper using the wrapper api and
   * pretty print turned off. */
  @Test(enabled=true)
  public void ResponseWrapperTest4()
  {
    // Get the completed wrapper as a string.
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.success, "hi, this is a message");
    String json = wrapper.addResult(new TestDTO(), false);
    System.out.println(json);
    
    // Check for expected json string.
    Assert.assertTrue(json.startsWith("{\"status\":\"success\",\"message\":\"hi, this is a message\",\"version\":"), "Unexpected wrapped DTO start");
    Assert.assertTrue(json.endsWith(",\"result\":{\"string1\":\"xxx\",\"int1\":88,\"bool1\":true,\"map1\":{\"Four\":4,\"Two\":2},\"instanceArray\":[\"1970-01-01T00:00:00Z\",\"+1000000000-12-31T23:59:59.999999999Z\"]}}"), 
                      "Unexpected wrapped DTO end");
  }
  
  /* ---------------------------------------------------------------------------- */
  /* testJobSubmitRequest5:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Test adding a result object to a response wrapper using the wrapper api and
   * pretty print turned on. */
  @Test(enabled=true)
  public void ResponseWrapperTest5()
  {
    // Get the completed wrapper as a string.
    ResponseWrapper wrapper = new ResponseWrapper(RESPONSE_STATUS.success, "hi, this is a message");
    String json = wrapper.addResult(new TestDTO(), true);
    System.out.println(json);
    
    // Construct formatted result without tapis version.
    String expectedStart = "{\n" + 
        "  \"status\": \"success\",\n" + 
        "  \"message\": \"hi, this is a message\",\n" + 
        "  \"version\": ";
    String expectedEnd = 
            "  \"result\": {\n" + 
            "    \"string1\": \"xxx\",\n" + 
            "    \"int1\": 88,\n" + 
            "    \"bool1\": true,\n" + 
            "    \"map1\": {\n" + 
            "      \"Four\": 4,\n" + 
            "      \"Two\": 2\n" + 
            "    },\n" + 
            "    \"instanceArray\": [\n" + 
            "      \"1970-01-01T00:00:00Z\",\n" + 
            "      \"+1000000000-12-31T23:59:59.999999999Z\"\n" + 
            "    ]\n" + 
            "  }\n" + 
            "}";
    
    // Check for expected json string ignoring the tapis version number.
    Assert.assertTrue(json.startsWith(expectedStart), "Unexpected wrapped DTO start");
    Assert.assertTrue(json.endsWith(expectedEnd), "Unexpected wrapped DTO end");
  }
}

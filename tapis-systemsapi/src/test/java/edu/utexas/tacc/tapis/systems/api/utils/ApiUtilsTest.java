package edu.utexas.tacc.tapis.systems.api.utils;

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ApiUtilsTest
{

  @BeforeMethod
  public void setUp()
  {
  }

  @AfterMethod
  public void tearDown()
  {
  }

  /*
   * Test method that takes a json element and returns a string value
   */
  @Test(groups={"unit"})
  public void testGetValS()
  {
    JsonElement jelem = null;
    // Verify passing in null returns default value
    String testStr = ApiUtils.getValS(jelem, "default");
    assertEquals(testStr, "default", "Default value should be returned with null");
    jelem = TapisGsonUtils.getGson().fromJson("notdefault", JsonElement.class);
    // Check that non-default value is returned
    testStr = ApiUtils.getValS(jelem, "default");
    assertEquals(testStr, "notdefault",  "Incorrect string returned");
  }

}
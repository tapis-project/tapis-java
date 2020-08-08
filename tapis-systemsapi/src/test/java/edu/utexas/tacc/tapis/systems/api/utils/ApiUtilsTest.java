package edu.utexas.tacc.tapis.systems.api.utils;

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

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

  /*
   * Check valid cases
   */
  @Test(groups={"unit"})
  public void testValidateAndExtractSearchListValid()
  {
    // Create all input and validation data for tests
    class ValidCaseOutput{public final int count; public final String[] results; ValidCaseOutput(int c, String[] r) {count=c;results=r;}}
    String[] validCaseInputs = {
      "enabled.eq.true",
      "(port.lt.7)",
      "(host.eq.stampede2.tacc.utexas.edu)~(default_access_method.in.PKI_KEYS,ACCESS_KEY)",
      "(enabled.eq.true)~(owner.eq.jdoe)~(proxy_port.lt.7)~(system_type.in.OBJECT_STORE,LINUX)",
      "(enabled.eq.true)~(port.lt.7)~(system_type.in.OBJECT_STORE,LINUX)~(description.like.my\\~system)", // ~ in value
      "(enabled.eq.true)~(port.gte.7)~(description.like.my\\ system)~(system_type.in.OBJECT_STORE,LINUX)", // space in value
//  TODO  \)  "(description.like.my\\\\\\,\\(\\)\\*\\!system)~(port.lte.7)~(system_type.in.OBJECT_STORE)", // 6 special chars in value: \ , ( ) * !
      "(description.like.my\\\\\\,\\(\\*\\!system)~(port.lte.7)~(system_type.in.OBJECT_STORE)", // 6 special chars in value: \ , ( ) * !
      "()~( )~()",
      "()~()",
      "~()~",
      "()~",
      "~()",
      "()",
      "(   )",
      "~~~",
      "~",
      "",
      null
    };
    ValidCaseOutput[] validCaseOutputs = {
      new ValidCaseOutput(1, new String[] {"enabled.eq.true"}),
      new ValidCaseOutput(1, new String[] {"port.lt.7"}),
      new ValidCaseOutput(2, new String[] {"host.eq.stampede2.tacc.utexas.edu","default_access_method.in.PKI_KEYS,ACCESS_KEY"}),
      new ValidCaseOutput(4, new String[] {"enabled.eq.true","owner.eq.jdoe","proxy_port.lt.7","system_type.in.OBJECT_STORE,LINUX"}),
      new ValidCaseOutput(4, new String[] {"enabled.eq.true","port.lt.7","system_type.in.OBJECT_STORE,LINUX","description.like.my\\~system"}),
      new ValidCaseOutput(4, new String[] {"enabled.eq.true","port.gte.7","description.like.my\\ system","system_type.in.OBJECT_STORE,LINUX"}),
// TODO \)     new ValidCaseOutput(4, new String[] {"description.like.my\\\\\\,\\(\\)\\*\\!system","port.lte.7","system_type.in.OBJECT_STORE"}),
      new ValidCaseOutput(3, new String[] {"description.like.my\\\\\\,\\(\\*\\!system","port.lte.7","system_type.in.OBJECT_STORE"}),
      new ValidCaseOutput(0, null), // "()~( )~()",
      new ValidCaseOutput(0, null), // "()~()",
      new ValidCaseOutput(0, null), // "~()~",
      new ValidCaseOutput(0, null), // "()~",
      new ValidCaseOutput(0, null), // "~()",
      new ValidCaseOutput(0, null), // "()",
      new ValidCaseOutput(0, null), // "(  )"
      new ValidCaseOutput(0, null), // "~~~",
      new ValidCaseOutput(0, null), // "~",
      new ValidCaseOutput(0, null), // ""
      new ValidCaseOutput(0, null) // null
    };

    // Iterate over other test cases
    //    for (int i = 0; i < validCaseInputs.length; i++)
    for (int i = 0; i < validCaseInputs.length; i++)
    {
      String searchListStr = validCaseInputs[i];
      System.out.println("Checking case # "+ i + " Input: " + searchListStr);
      List<String> searchResults = ApiUtils.validateAndExtractSearchList(searchListStr);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), validCaseOutputs[i].count);
      for (int j = 0; j < validCaseOutputs[i].count; j++)
      {
        System.out.println("  Result string # " + j + " = " + searchResults.get(j));
        assertEquals(searchResults.get(j), validCaseOutputs[i].results[j]);
      }
    }
  }

  /*
   * Check invalid cases
   */
  @Test(groups={"unit"})
  public void testValidateAndExtractSearchListInvalid()
  {
    // Create all input and validation data for tests
    String[] invalidCaseInputs = {
      "(enabled.eq.true",
      "port.lt.7)",
      "enabled.eq.true~port.lt.7",
      "(host.eq.stampede2.tacc.utexas.edu)~default_access_method.in.PKI_KEYS,ACCESS_KEY",
      "(enabled.eq.true)~owner.eq.jdoe)~(proxy_port.lt.7)~(system_type.in.OBJECT_STORE,LINUX)",
      "(enabled.eq.true)~(~(system_type.in.OBJECT_STORE,LINUX)",
      "(enabled.eq.true)~)~(system_type.in.OBJECT_STORE,LINUX)",
      ".eq.true",
      "true",
      "enabled.true",
      "1enabled.eq.true",
      "en$abled.eq.true",
      "(enabled.equal.true)",
      "(port.l@t.7)",
      "(host.eq.myhost)~(default_access_method.in.)",
      "(enabled.eq.true)~(proxy_port.lt.7)~(system_type.in)",
      "(enabled.eq.)~(proxy_port.lt.7)~(system_type.in.OBJECT_STORE,LINUX)",
      "(enabled.eq.true)~(proxy_port.lt.)~(system_type.in.OBJECT_STORE,LINUX)",
      "(enabled.eq.true)~(proxy_port.lt.7)~(system_type.in.)"
    };

    // Iterate over other test cases
    //    for (int i = 0; i < validCaseInputs.length; i++)
    for (int i = 0; i < invalidCaseInputs.length; i++)
    {
      String searchListStr = invalidCaseInputs[i];
      System.out.println("Checking case # "+ i + " Input: " + searchListStr);
      try
      {
        List<String> searchResults = ApiUtils.validateAndExtractSearchList(searchListStr);
        System.out.println("  Result size: " + searchResults.size());
        fail("Expected IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
        System.out.println("Expected exception: " + e);
      }
    }
    // TODO: Explicitly check for certain exceptions rather than just that an IllegalArg has been thrown.
    //       E.g., (port.l@t.7) should throw an exception containing SYSAPI_SEARCHCOND_INVALID_OP
  }
}
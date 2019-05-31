package edu.utexas.tacc.tapis.shared.jsonschema.twolevel;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.schema.ResourceSchemaClient;

@Test(groups={"unit"})
public class TwoLevelBuilderTest 
{
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  private static final String INPUT_FILE = 
      "/edu/utexas/tacc/tapis/shared/jsonschema/twolevel/parent.json";
  
  /* **************************************************************************** */
  /*                                    Tests                                     */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* testTwoLevelBuilder:                                                         */
  /* ---------------------------------------------------------------------------- */
  /** This test uses a schema configured in parent and child files.  We have to use
   * our custom schema client class to resolve the child schema reference in the 
   * parent schema. 
   * 
   * This test uses the fluent API calls including one that specifies draft 6 conformance.
   */
  @Test(enabled=true)
  public void testTwoLevelBuilder()
  {
    Schema schema = SchemaLoader.builder()
        .draftV6Support()
        .schemaJson(new JSONObject(new JSONTokener(getClass().getResourceAsStream(INPUT_FILE))))
        .httpClient(new ResourceSchemaClient())
        .build().load().build();
    
    schema.validate(new JSONObject("{\"parent\" : \"Bud\", \"child\" : {\"name\" : \"Harry\", \"num\" : 77}}"));
  }
 
}

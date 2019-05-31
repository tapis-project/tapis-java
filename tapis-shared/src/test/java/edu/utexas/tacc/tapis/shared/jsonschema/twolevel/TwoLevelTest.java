package edu.utexas.tacc.tapis.shared.jsonschema.twolevel;

import java.io.InputStream;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.schema.ResourceSchemaClient;

@Test(groups={"unit"})
public class TwoLevelTest 
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
  /* testTwoLevel:                                                                */
  /* ---------------------------------------------------------------------------- */
  /** This test uses a schema configured in parent and child files.  We have to use
   * our custom schema client class to resolve the child schema reference in the 
   * parent schema. 
   * 
   */
  @Test(enabled=true)
  public void testTwoLevel()
  {
    InputStream ins = getClass().getResourceAsStream(INPUT_FILE);
    JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
    Schema schema = SchemaLoader.load(rawSchema, new ResourceSchemaClient());
    schema.validate(new JSONObject("{\"parent\" : \"Bud\", \"child\" : {\"name\" : \"Harry\", \"num\" : 77}}"));
  }
 
}

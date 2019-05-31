package edu.utexas.tacc.tapis.shared.jsonschema.simple;

import java.io.InputStream;

import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.testng.annotations.Test;

@Test(groups={"unit"})
public class SimpleTest 
{
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  private static final String INPUT_FILE = 
      "/edu/utexas/tacc/tapis/shared/jsonschema/simple/simple.json";
  
  /* **************************************************************************** */
  /*                                    Tests                                     */
  /* **************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* testLog:                                                                    */
  /* --------------------------------------------------------------------------- */
  @Test(enabled=true)
  public void testSimple()
  {
    InputStream ins = getClass().getResourceAsStream(INPUT_FILE);
    JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
    org.everit.json.schema.Schema schema = SchemaLoader.load(rawSchema);
    schema.validate(new JSONObject("{\"name\" : \"Bud\", \"num\" : 5}"));
  }
  
}

package edu.utexas.tacc.tapis.shared.schema;

import java.io.InputStream;
import java.util.List;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class is a repository for the json schema objects that are used throughout
 * the Tapis services.  Each schema object is created when it is first requested and
 * cached for subsequent uses.
 * 
 * To add a new schema to this utility class, do the following:
 * 
 *    1. Assign a new FILE_* string constant the path name of the schema resource file.
 *    2. Create a private static Schema field for the schema object.
 *    3. Create a public static validate* method for clients to use.
 * 
 * Note that the validate methods do not have to be synchronized.  If a method 
 * executes concurrently on multiple threads before a schema field is assigned,
 * the last one assigned wins.  This is safe since both objects are the same
 * and object references (i.e., addresses) are atomically assigned by the JVM.
 * Once initialization ends, race conditions will not occur since all clients
 * will read the schema field but not try to write it.  Schema objects themselves
 * are thread-safe.
 * 
 * @author rcardone
 */
public class JsonValidator 
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(JsonValidator.class);
  
  // -------- Schema resource file URIs.
  private static String FILE_SAMPLE_CREATE_REQUEST = "/edu/utexas/tacc/tapis/sample/api/jsonschema/SampleCreateRequest.json";
  private static String FILE_SK_CREATE_ROLE_REQUEST = "/edu/utexas/tacc/tapis/security/api/jsonschema/CreateRoleRequest.json";
  
  // -------- Demand-loaded schema objects. 
  private static Schema _sampleCreateRequestSchema;
  private static Schema _skCreateRoleRequestSchema;
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* Add a new validate method for each schema. */
  
  /* ---------------------------------------------------------------------------- */
  /* validateSampleCreateRequest:                                                 */
  /* ---------------------------------------------------------------------------- */
  public static void validateSampleCreateRequest(String json) throws TapisJSONException
  {
    Schema schema = getSampleCreateRequestSchema();
    try {schema.validate(new JSONObject(json));}
        catch (ValidationException e) {
            // Get the detailed list of parse failures. 
            // The returned list is never empty.
            ValidationException e1 = (ValidationException)e;
            List<String> messages = e1.getAllMessages();
            String details = "";
            int i = 1;
            for (String s : messages) details += " #" + (i++) + s;
        
            // Log the exception details.
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_FAILURE", e.getMessage(), details);
            _log.error(msg, e);
            throw new TapisJSONException(msg, e);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisJSONException(msg, e);
      }
  }

  /* ---------------------------------------------------------------------------- */
  /* validateSkCreateRoleRequest:                                                 */
  /* ---------------------------------------------------------------------------- */
  public static void validateSkCreateRoleRequest(String json) throws TapisJSONException
  {
    Schema schema = getSkCreateRoleRequestSchema();
    try {schema.validate(new JSONObject(json));}
        catch (ValidationException e) {
            // Get the detailed list of parse failures. 
            // The returned list is never empty.
            ValidationException e1 = (ValidationException)e;
            List<String> messages = e1.getAllMessages();
            String details = "";
            int i = 1;
            for (String s : messages) details += " #" + (i++) + s;
        
            // Log the exception details.
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_FAILURE", e.getMessage(), details);
            _log.error(msg, e);
            throw new TapisJSONException(msg, e);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisJSONException(msg, e);
      }
  }

  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getSchema:                                                                   */
  /* ---------------------------------------------------------------------------- */
  private static Schema getSchema(String schemaFile) throws TapisJSONException 
  {
    // Load the schema as a resource.
    Schema schema = null;
    try (InputStream ins = JsonValidator.class.getResourceAsStream(schemaFile)) {
      JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
      schema = SchemaLoader.load(rawSchema, new ResourceSchemaClient());
    }
    catch (Exception e) {
      String msg = MsgUtils.getMsg("TAPIS_JSON_SCHEMA_LOAD_ERROR", schemaFile, e.getMessage());
      _log.error(msg, e);
      throw new TapisJSONException(msg, e);
    }

    return schema;
  }

  /* **************************************************************************** */
  /*                            Schema Loading Methods                            */
  /* **************************************************************************** */
  /* Add a new load method for each schema. */
  
  /* ---------------------------------------------------------------------------- */
  /* getSampleCreateRequestSchema:                                                */
  /* ---------------------------------------------------------------------------- */
  private static Schema getSampleCreateRequestSchema() throws TapisJSONException 
  {
    if (_sampleCreateRequestSchema == null) 
      _sampleCreateRequestSchema = getSchema(FILE_SAMPLE_CREATE_REQUEST);
    return _sampleCreateRequestSchema;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getSkCreateRoleRequestSchema:                                                */
  /* ---------------------------------------------------------------------------- */
  private static Schema getSkCreateRoleRequestSchema() throws TapisJSONException 
  {
    if (_skCreateRoleRequestSchema == null) 
        _skCreateRoleRequestSchema = getSchema(FILE_SK_CREATE_ROLE_REQUEST);
    return _skCreateRoleRequestSchema;
  }

}

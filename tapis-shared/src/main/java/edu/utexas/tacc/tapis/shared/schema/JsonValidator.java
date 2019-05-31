package edu.utexas.tacc.tapis.shared.schema;

import java.io.InputStream;
import java.util.List;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.AloeJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class is a repository for the json schema objects that are used throughout
 * the Aloe services.  Each schema object is created when it is first requested and
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
  private static String FILE_JOB_SUBMIT_REQUEST = "/edu/utexas/tacc/tapis/jobs/api/jsonschema/JobSubmitRequest.json";
  private static String FILE_JOB_QUEUE_DEFS     = "/edu/utexas/tacc/tapis/jobs/jsonschema/JobQueueDefinitions.json";
  
  // -------- Demand-loaded schema objects. 
  private static Schema _jobSubmitRequestSchema;
  private static Schema _jobQueueDefinitionsSchema;
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* validateJobSubmitRequest:                                                    */
  /* ---------------------------------------------------------------------------- */
  public static void validateJobSubmitRequest(String json) throws AloeJSONException
  {
    Schema schema = getJobSubmitRequestSchema();
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
            String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_FAILURE", e.getMessage(), details);
            _log.error(msg, e);
            throw new AloeJSONException(msg, e);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new AloeJSONException(msg, e);
      }
  }

  /* ---------------------------------------------------------------------------- */
  /* validateJobQueueDefinitions:                                                 */
  /* ---------------------------------------------------------------------------- */
  public static void validateJobQueueDefinitions(String json) throws AloeJSONException
  {
    Schema schema = getJobQueueDefinitionsSchema();
    try {schema.validate(new JSONArray(json));}
      catch (ValidationException e) {
          // Get the detailed list of parse failures. 
          // The returned list is never empty.
          ValidationException e1 = (ValidationException)e;
          List<String> messages = e1.getAllMessages();
          String details = "";
          int i = 1;
          for (String s : messages) details += " #" + (i++) + s;
          
          // Log the exception details.
          String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_FAILURE", e.getMessage(), details);
          _log.error(msg, e);
          throw new AloeJSONException(msg, e);
      }
      catch (Exception e) {
          String msg = MsgUtils.getMsg("ALOE_JSON_VALIDATION_ERROR", e.getMessage());
          _log.error(msg, e);
          throw new AloeJSONException(msg, e);
      }
  }

  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getSchema:                                                                   */
  /* ---------------------------------------------------------------------------- */
  private static Schema getSchema(String schemaFile) throws AloeJSONException 
  {
    // Load the schema as a resource.
    Schema schema = null;
    try (InputStream ins = JsonValidator.class.getResourceAsStream(schemaFile)) {
      JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
      schema = SchemaLoader.load(rawSchema, new ResourceSchemaClient());
    }
    catch (Exception e) {
      String msg = MsgUtils.getMsg("ALOE_JSON_SCHEMA_LOAD_ERROR", schemaFile, e.getMessage());
      _log.error(msg, e);
      throw new AloeJSONException(msg, e);
    }

    return schema;
  }

  /* **************************************************************************** */
  /*                            Schema Loading Methods                            */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getJobSubmitRequestSchema:                                                   */
  /* ---------------------------------------------------------------------------- */
  private static Schema getJobSubmitRequestSchema() throws AloeJSONException 
  {
    if (_jobSubmitRequestSchema == null) 
      _jobSubmitRequestSchema = getSchema(FILE_JOB_SUBMIT_REQUEST);
    return _jobSubmitRequestSchema;
  }

  /* ---------------------------------------------------------------------------- */
  /* getJobQueueDefinitionsSchema:                                                */
  /* ---------------------------------------------------------------------------- */
  private static Schema getJobQueueDefinitionsSchema() throws AloeJSONException 
  {
    if (_jobQueueDefinitionsSchema == null) 
      _jobQueueDefinitionsSchema = getSchema(FILE_JOB_QUEUE_DEFS);
    return _jobQueueDefinitionsSchema;
  }
}

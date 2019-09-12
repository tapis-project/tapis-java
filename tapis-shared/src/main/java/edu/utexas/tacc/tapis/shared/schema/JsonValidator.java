package edu.utexas.tacc.tapis.shared.schema;

import java.io.InputStream;
import java.util.HashMap;
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
 * cached for subsequent uses.  The total number of schemas cached for a service is
 * expected to be small so cache eviction is not necessary. 
 * 
 * Note that the validate methods do not have to be synchronized since we are
 * synchronizing cache access.  Schema objects themselves are thread-safe.
 * 
 * @author rcardone
 */
public final class JsonValidator 
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(JsonValidator.class);
  
  //-------- Demand-loaded schema objects.
  private static final HashMap<String,Schema> _schemaCache = new HashMap<>();
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* validateSampleCreateRequest:                                                 */
  /* ---------------------------------------------------------------------------- */
  public static void validate(JsonValidatorSpec spec) throws TapisJSONException
  {
    Schema schema = getSchema(spec.getSchemaFile());
    try {schema.validate(new JSONObject(spec.getJson()));}
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
  private static synchronized Schema getSchema(String schemaFile) 
   throws TapisJSONException 
  {
    // Use the cached schema if it exists, otherwise create it.
    // All cache access is synchronized.
    Schema schema = _schemaCache.get(schemaFile);
    if (schema == null) {
      schema = loadSchema(schemaFile);
      _schemaCache.put(schemaFile, schema);
    }
    return schema;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* loadSchema:                                                                  */
  /* ---------------------------------------------------------------------------- */
  private static Schema loadSchema(String schemaFile) throws TapisJSONException 
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
}

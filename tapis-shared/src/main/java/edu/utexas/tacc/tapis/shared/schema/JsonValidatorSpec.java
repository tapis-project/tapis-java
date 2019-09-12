package edu.utexas.tacc.tapis.shared.schema;

public final class JsonValidatorSpec 
{
    // Fields.
    private final String _json;       // json to be validated
    private final String _schemaFile; // resource file path to json schema
    
    // Constructor.
    public JsonValidatorSpec(String json, String schemaFile) 
    {
        _json = json;
        _schemaFile = schemaFile;
    }
    
    // Accessors.
    public String getJson() {
        return _json;
    }
    public String getSchemaFile() {
        return _schemaFile;
    }
}

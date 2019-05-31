package edu.utexas.tacc.tapis.shared.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.gson.javatime.Converters;

public class TapisGsonUtils 
{
	/* **************************************************************************** */
	/*                                Public Methods                                */
	/* **************************************************************************** */
	/* ---------------------------------------------------------------------------- */
	/* getGsonBuilder:                                                              */
	/* ---------------------------------------------------------------------------- */
	/** Provide a gson builder with registered serializers and deserializers.  This
	 * method relies on open source software from the gson-javatime-serialisers project
	 * to format Java date/time objects reasonably.  In addition, we always serialize 
	 * nulls. 
	 * Disabled HTML escaping of characters such as =, <, >, & . That was an requirement for 
	 * _links object in the job submission response. A detailed discussion on HTML escaping in
	 *  gson can be found in: https://groups.google.com/forum/#!topic/google-gson/JDHUo9DWyyM\ .
	 * 
	 * @param prettyPrint true to turn on pretty printing, false otherwise
	 * @return a gson builder
	 */
	public static GsonBuilder getGsonBuilder(boolean prettyPrint)
	{
		// Set the date/time translators.
		GsonBuilder builder = new GsonBuilder().serializeNulls().disableHtmlEscaping();
		if (prettyPrint) builder.setPrettyPrinting();
		Converters.registerAll(builder);
		
		return builder;
	}

	/* ---------------------------------------------------------------------------- */
	/* getGson:                                                                     */
	/* ---------------------------------------------------------------------------- */
	/** Provide a gson object with registered serializers and deserializers.
	 * Pretty printing is turned off by default.
	 * 
	 * @return a gson object
	 */
	public static Gson getGson()
	{
		return getGsonBuilder(false).create();
	}
	
    /* ---------------------------------------------------------------------------- */
    /* getGson:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Provide a gson object with registered serializers and deserializers.
     * Specify whether or not to turn on pretty printing.
     * 
     * @return a gson object
     */
    public static Gson getGson(boolean prettyPrint)
    {
        return getGsonBuilder(prettyPrint).create();
    }

    /* ---------------------------------------------------------------------------- */
    /* getGson:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Add an element to an existing json object allowing for some flexibility on
     * how strings are interpreted.
     * 
     * @param obj the target json object that receives new element
     * @param key the name of the element
     * @param value the value of the element
     * @return the modified target object
     */
    public static JsonObject addTo(JsonObject obj, String key, Object value)
    {
        // Add the key/value pair to the target object.
        if (value == null) {
            obj.add(key, JsonNull.INSTANCE);
        }
        // Process based on type.
        else if (value instanceof JsonElement) {
            obj.add(key, (JsonElement)value);
        }
        else if (value instanceof String) {
            // Strings can represent json arrays, objects or just plain strings.
            // Get rid unnecessary spaces.
            String v = ((String) value).trim();
            if (v.startsWith("[")){
                 Gson gson = getGson();
                 obj.add(key, gson.fromJson(v, JsonArray.class));
             }
            else if (v.startsWith("{")){
                Gson gson = getGson();
                obj.add(key, gson.fromJson(v, JsonObject.class));
            }
            else obj.addProperty(key, (String)v);
        }
        else if (value instanceof Number) {
            obj.addProperty(key, (Number)value);
        }
        else if (value instanceof Boolean) {
            obj.addProperty(key, (Boolean)value);
        }
        else if (value instanceof Character) {
            obj.addProperty(key, (Character)value);
        }
            
        return obj;
    }
}

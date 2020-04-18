package edu.utexas.tacc.tapis.systems.model;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/*
 * Class representing simple unstructured metadata formatted as json.
 * Immutable
 * Please keep it immutable.
 *
 */
public final class Notes
{
  private final String data;
  private final JsonObject json;
  public Notes(String d)
  {
    data = d;
    json = TapisGsonUtils.getGson().fromJson(d, JsonObject.class);
  }
  public String getData() { return data; }
  public JsonObject getJson() { return json; }
}

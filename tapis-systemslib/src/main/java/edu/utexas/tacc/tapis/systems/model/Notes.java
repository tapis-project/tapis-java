package edu.utexas.tacc.tapis.systems.model;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.apache.commons.lang3.StringUtils;

import static java.lang.System.*;

/*
 * Class representing simple unstructured metadata formatted as json.
 * Immutable
 * Please keep it immutable.
 *
 */
public final class Notes
{
  private final String stringData;
  private final JsonElement jsonData;
  public Notes(String d)
  {
    String tmpStr = StringUtils.trimToEmpty(d);
    // Add surrounding curly braces if not there
    if (!tmpStr.startsWith("{")) tmpStr = "{" + tmpStr + "}";
    stringData = tmpStr;
    jsonData = TapisGsonUtils.getGson().fromJson(tmpStr, JsonElement.class);
  }
  public String getStringData() { return stringData; }
  public JsonElement getJsonData() { return jsonData; }
}

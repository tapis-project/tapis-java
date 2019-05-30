package edu.utexas.tacc.aloe.shared.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

@Test(groups={"unit"})
public class AloeGsonUtilsTest 
{
    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* addToTest:                                                                   */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void addToTest()
    {
        // Get a parser.
        Gson gson = AloeGsonUtils.getGson(true);
        
        // --- Create the target object.
        JsonObject obj = new JsonObject();
        Assert.assertEquals(obj.toString(), "{}");
        
        // --- Add a string.
        AloeGsonUtils.addTo(obj, "string1", "some string");
        Assert.assertEquals(obj.toString(), "{\"string1\":\"some string\"}");
        
        // --- Add a number.
        AloeGsonUtils.addTo(obj, "num1", 66);
        Assert.assertEquals(obj.toString(), "{\"string1\":\"some string\",\"num1\":66}");
        
        // --- Add a number.
        AloeGsonUtils.addTo(obj, "num2", -45.998);
        Assert.assertEquals(obj.toString(), "{\"string1\":\"some string\",\"num1\":66,\"num2\":-45.998}");
        
        // --- Add a boolean.
        AloeGsonUtils.addTo(obj, "bool", true);
        Assert.assertEquals(obj.toString(), "{\"string1\":\"some string\",\"num1\":66,\"num2\":-45.998,\"bool\":true}");
        
        // --- Add a character.
        AloeGsonUtils.addTo(obj, "char", 'c');
        Assert.assertEquals(obj.toString(), "{\"string1\":\"some string\",\"num1\":66,\"num2\":-45.998,\"bool\":true,\"char\":\"c\"}");
        
        // --- Add an array as a string.
        AloeGsonUtils.addTo(obj, "array1", "[\"banana\", \"apple\"]");
        String expected = "{\"string1\":\"some string\",\"num1\":66,\"num2\":-45.998,\"bool\":true,\"char\":\"c\",\"array1\":[\"banana\",\"apple\"]}";
        Assert.assertEquals(obj.toString(), expected);
        
        // --- Add an object as a string.
        AloeGsonUtils.addTo(obj, "object1", "{\"key1\": \"val1\", \"key2\": \"val2\"}");
        expected = "{\"string1\":\"some string\",\"num1\":66,\"num2\":-45.998,\"bool\":true,\"char\":\"c\",\"array1\":[\"banana\",\"apple\"],\"object1\":{\"key1\":\"val1\",\"key2\":\"val2\"}}";
        Assert.assertEquals(obj.toString(), expected);
        
        // --- Add nested nonsense as a string.
        AloeGsonUtils.addTo(obj, "array2", "[\"cherry\", {\"key1\": \"val1\"}, [{\"x\":\"y\"}, {\"u\":\"v\"}]]");
        expected = "{\"string1\":\"some string\",\"num1\":66,\"num2\":-45.998,\"bool\":true,\"char\":\"c\",\"array1\":[\"banana\",\"apple\"],\"object1\":{\"key1\":\"val1\",\"key2\":\"val2\"},\"array2\":[\"cherry\",{\"key1\":\"val1\"},[{\"x\":\"y\"},{\"u\":\"v\"}]]}";
        Assert.assertEquals(obj.toString(), expected);
        
        // Convert the last string to a json object to demonstrate  
        // that we can easily switch between the two representations.
        JsonObject newObj = gson.fromJson(expected, JsonObject.class);
        Assert.assertEquals(newObj.toString(), expected);
        
//        System.out.println(gson.toJson(obj));
//        System.out.println(obj);
    }

}

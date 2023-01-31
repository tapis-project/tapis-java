package edu.utexas.tacc.tapis.jobs;

import org.testng.annotations.Test;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** These tests can be run manually to help figure out how the openapi 
 * generated json code will behave.
 * 
 * @author rcardone
 */
@Test(groups={"manual"})
public class JsonEmbeddingTest 
{
    @Test
    public void notesTest() 
    {
        final String notes = 
          "{\"fname\":\"bud\",\"lname\":\"jones\",\"count\":27.0,\"happy\":true,\"hello\":null,"
          + "\"obj\":{\"f1\":\"v1\",\"f2\":44.0},\"array\":[\"x\",\"y\",\"z\"]}";  
        
        // println removes embedding
        System.out.println(1);
        System.out.println(notes);
        
        // toJson keeps embedding.
        System.out.println(2);
        System.out.println(TapisGsonUtils.getGson().toJson(notes));
        
        // JsonObject.toString() removes embedding.
        System.out.println(3);
        System.out.println(TapisGsonUtils.getGson().fromJson(notes, JsonObject.class).toString());
        
        
        System.out.println();
    }

    @Test
    public void ArgSpecTest() 
    {
        final String notes = 
          "{\"fname\":\"bud\",\"lname\":\"jones\",\"count\":27.0,\"happy\":true,\"hello\":null,"
          + "\"obj\":{\"f1\":\"v1\",\"f2\":44.0},\"array\":[\"x\",\"y\",\"z\"]}";  
        
        final var arg = new ArgSpec("bud", 1, notes);
        
        // toJson keeps notes embedding.
        System.out.println(1);
        System.out.println(TapisGsonUtils.getGson().toJson(arg));
        
        // JsonElement.toString() does not remove notes embedding.
        System.out.println(2);
        System.out.println(TapisGsonUtils.getGson().toJsonTree(arg, ArgSpec.class).toString());
        
        // JsonObject.toString() removes notes embedding.
        System.out.println(3);
        System.out.println(TapisGsonUtils.getGson().fromJson(arg.notes, JsonObject.class).toString());
        
        // toJsonStr acts likes a DTO and manually assembles the correct output.
        System.out.println(4);
        System.out.println(arg.toJsonStr());
        
        System.out.println("\n***** 2 level embedding tests *****");
        final var parms = new Parms();
        
        // toJson keeps notes embedding.
        System.out.println(10);
        System.out.println(TapisGsonUtils.getGson().toJson(parms));
        
        // JsonElement.toString() does not remove notes embedding.
        System.out.println(11);
        System.out.println(TapisGsonUtils.getGson().toJsonTree(parms, Parms.class).toString());

        // toJsonStr acts likes a DTO and manually assembles the correct output.
        System.out.println(12);
        System.out.println(parms.toJsonStr());
        
        System.out.println();
    }
    
    
    private static class ArgSpec {
        String name;
        int    cnt;
        String notes;
        
        ArgSpec(String n, int i, String notes){
            name = n;
            cnt  = i;
            this.notes = notes;
        }
        
        String toJsonStr() {
            var s = "\"name\":"+name+",\"ctn\":"+cnt+",\"notes\":"+
                    TapisGsonUtils.getGson().fromJson(notes, JsonObject.class).toString();
            return s;
        }
    }
    
    private static class Parms {
        static final String notes = 
                "{\"fname\":\"bud\",\"lname\":\"jones\",\"count\":56.0,\"happy\":true,\"hello\":null,"
                + "\"obj\":{\"f1\":\"v1\",\"f2\":88.0},\"array\":[\"x\",\"y\",\"z\"]}";  

        boolean outer = true;
        ArgSpec arg   = new ArgSpec("wally", 99, notes);
        
        String toJsonStr() {
            return "\"outer\":"+outer+",\"arg\":" + arg.toJsonStr();
        }
    }
}

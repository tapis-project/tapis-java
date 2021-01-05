package edu.utexas.tacc.tapis.jobs.utils;

import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"unit"})
public class MacroResovlerTest2 
{
    // Initialize fake macro definitions.
    private final HashMap<String,String> _macroMap = initMacroMap();
    
    @Test
    public void macroParseTest1() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "0123456789";
        String result = resolver.replaceMacros(text);
        Assert.assertEquals(result, text);
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest2() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "0123456789 ${} ${_plum} xx ${_apple}";
        String result = resolver.replaceMacros(text);
        Assert.assertEquals(result, "0123456789 ${} blue xx red");
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest3() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "${}1${_plum}2${_apple}3${www}";
        String result = resolver.replaceMacros(text);
        Assert.assertEquals(result, "${}1blue2red3${www}");
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest4() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "${}1${_plum}2${_apple}3${www}A";
        String result = resolver.replaceMacros(text);
        Assert.assertEquals(result, "${}1blue2red3${www}A");
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest5() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "${}${_plum}${_apple}${www}";
        String result = resolver.replaceMacros(text);
        Assert.assertEquals(result, "${}bluered${www}");
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest6() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "";
        String result = resolver.replaceMacros(text);
        Assert.assertEquals(result, text);
//        System.out.println(result);
    }

    /* ---------------------------------------------------------------------------- */
    /* initMacroMap:                                                                */
    /* ---------------------------------------------------------------------------- */
    private HashMap<String,String> initMacroMap()
    {
        var macroMap = new HashMap<String,String>();
        macroMap.put("_banana", "yellow");
        macroMap.put("_apple", "red");
        macroMap.put("_peach", "purple");
        macroMap.put("_plum", "blue");
        
        return macroMap;
    }
}

package edu.utexas.tacc.tapis.jobs.utils;

import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"unit"})
public class MacroResovlerTest 
{
    // Initialize fake macro definitions.
    private final HashMap<String,String> _macroMap = initMacroMap();
    
    @Test
    public void macroParseTest1() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "x$xx${_banana}/${_apple}/yy";
        String result = resolver.resolve(text);
        Assert.assertEquals(result, "x$xxyellow/red/yy");
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest2() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "x{$xx${_orange}/${_peach}/yy";
        String result = resolver.resolve(text);
        Assert.assertEquals(result, "x{$xxblue/red/pink/yellow/purple/yy");
//        System.out.println(result);
    }
    
    @Test
    public void macroParseTest3() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "${_tangerine}";
        String result = resolver.resolve(text);
        Assert.assertEquals(result, "aaablue/red/pink/yellow/red/purple");
//        System.out.println(result);
    }
    
    @Test(expectedExceptions = TapisException.class)
    public void macroParseTest4() throws TapisException
    {
        var resolver = new MacroResolver(null, _macroMap);
        String text = "${_kiwi}";
        String result = resolver.resolve(text);
    }
    
    @Test(expectedExceptions = TapisException.class)
    public void macroParseTest5() throws TapisException
    {
        // Doesn't recognize an empty macro definition.
        var resolver = new MacroResolver(null, _macroMap);
        String text = "$$$${}$$$";
        String result = resolver.resolve(text);
    }
    
    @Test(expectedExceptions = TapisException.class)
    public void macroParseTest6() throws TapisException
    {
        // Doesn't recognize an empty macro definition.
        var resolver = new MacroResolver(null, _macroMap);
        String text = "$$$${$$$";
        String result = resolver.resolve(text);
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
        macroMap.put("_plum", "blue/${_apple}");
        macroMap.put("_orange", "${_plum}/pink/${_banana}");
        macroMap.put("_tangerine", "aaa${_orange}/${_apple}/${_peach}");
        macroMap.put("_kiwi", "brown/${_banana}/${unknown}");
        
        return macroMap;
    }
}

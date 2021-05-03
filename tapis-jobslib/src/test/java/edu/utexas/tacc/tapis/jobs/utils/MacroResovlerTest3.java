package edu.utexas.tacc.tapis.jobs.utils;

import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"unit"})
public class MacroResovlerTest3 
{
    private static final Pattern _pattern = MacroResolver._envVarPattern;
    //private static final Pattern _pattern = Pattern.compile("(\\$?[a-zA-Z_][a-zA-Z0-9_]*)\\s*(,\\s*(\\S+)\\s*)?");
    
    @Test
    public void macroParseTest1() throws TapisException
    {
        var s = "$varname";
        var m = _pattern.matcher(s);
        var b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), s, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), null, "Failed on \""+ s + "\"");
                
        s = "varname";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), s, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), null, "Failed on \""+ s + "\"");
        
        s = "$varname,/path/name";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), "$varname", "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), "/path/name", "Failed on \""+ s + "\"");
        
        s = "$varname ,/path/name";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), "$varname", "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), "/path/name", "Failed on \""+ s + "\"");
        
        s = "$varname ,  /path/name";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), "$varname", "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), "/path/name", "Failed on \""+ s + "\"");
        
        s = "$varname ,  /path/name   ";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), "$varname", "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), "/path/name", "Failed on \""+ s + "\"");
        
        s = "$varname ";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, true, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.groupCount(), 3, "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(1), "$varname", "Failed on \""+ s + "\"");
        Assert.assertEquals(m.group(3), null, "Failed on \""+ s + "\"");

        s = "$varname,";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, false, "Failed on \""+ s + "\"");
        
        s = "$varname   /path/name";
        m = _pattern.matcher(s);
        b = m.matches();
        Assert.assertEquals(b, false, "Failed on \""+ s + "\"");
    }
    
 }

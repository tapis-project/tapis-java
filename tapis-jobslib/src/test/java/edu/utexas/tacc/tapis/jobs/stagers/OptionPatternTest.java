package edu.utexas.tacc.tapis.jobs.stagers;

import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups={"unit"})
public class OptionPatternTest 
{
    // Get the option pattern regex.
    private static final Pattern _optionPattern = AbstractJobExecStager._optionPattern;
    //private static final Pattern _optionPattern = Pattern.compile("\\s*(--?[^=\\s]*)\\s*=?\\s*(\\S*)\\s*");
    
    @Test
    public void ParseTest1()
    {
        // Happy path test...
        String arg = "-w /TapisInput";
        var m = _optionPattern.matcher(arg);
        boolean matches = m.matches();
        Assert.assertEquals(matches, true);
        
        int groupCount = m.groupCount();
        Assert.assertEquals(groupCount, 2, "Expected 2 groups, got " + groupCount);
        Assert.assertEquals(m.group(1), "-w");
        Assert.assertEquals(m.group(2), "/TapisInput");
        
//        System.out.println("groupCount: " + groupCount);
//        for (int i = 0; i <= groupCount; i++) System.out.println(" " + i + ": " + m.group(i));
    }
}

package edu.utexas.tacc.tapis.jobs.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public final class MacroResolver 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(MacroResolver.class);
    
    // Host function.
    public static final String HOST_EVAL_PREFIX = "HOST_EVAL(";
    
    // The character sequence that indicates the beginning of a macro definition.
    public static final String MACRO_DELIMITER = "${";
    
    // Maximum number top level iterations allowed when resolving macros.
    private static final int MAX_ITERATIONS = 16;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Used to access host environment variables.
    private final TSystem _execSystem;
    private final Map<String,String> _macros;
    
    // Cache of environment variable values retrieved from the execution system.
    private final HashMap<String,String> _hostVariables = new HashMap<String, String>();
;    
    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** The execSystem will be used to retrieve environment variable values from the
     * execution system as the job's authenticated user.  If execSystem is null, then
     * retrieval is skipped and HOST_EVAL function won't be replaced.  execSystem is
     * not null during normal job processing.
     * 
     * @param execSystem the execution system or null for testing
     * @param macros non-null mapping of resolved macro names to their values
     */
    public MacroResolver(TSystem execSystem, Map<String,String> macros)
    {
        _execSystem = execSystem;
        _macros = macros;
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* resolve:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Return the string with any host function or macro definitions replaced by
     * their values.  If a string cannot be resolved an exception is thrown.
     * 
     * @param text a string that may contain a host function or one or more macro definitions
     * @return the string with all placeholder values replaced
     * @throws TapisException when resolution fails for any reason
     */
    public String resolve(String text) throws TapisException
    {
        // Resolve the host function and then all macros.
        return replaceAllMacros(replaceHostEval(text));
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* replaceHostEval:                                                             */
    /* ---------------------------------------------------------------------------- */
    private String replaceHostEval(String text)
    {
        // Do we need to evaluate a host environment variable?
        if (_execSystem == null || !text.startsWith(HOST_EVAL_PREFIX)) return text;
        
        
        
        
        return text;
    }

    /* ---------------------------------------------------------------------------- */
    /* replaceAllMacros:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Replace all macros in the text with their values as defined in the current
     * _macros map.  This method does not change the map so the map should contain
     * any values that may be referenced, directly or transitively, from macros
     * contained in the text.
     * 
     * By calling a recursive routine, this method supports macros that resolve to 
     * strings that themselves can contain macros.  The number of calls to the recursive
     * routine are limited so as to avoid cycles not easily detected during recursion.  
     * 
     * @param text that non-empty text that may contain one or more macros
     * @return a string with all macro definitions replaced with their concrete values
     * @throws TapisException if resolution cannot complete
     */
    private String replaceAllMacros(String text) throws TapisException
    {
        // Check input.
        if (StringUtils.isBlank(text)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replaceAllMacros", "text");
            throw new TapisException(msg);
        }
        
        // Set up recursive processing.
        final String originalText = text;
        var macrosResolved = new ArrayList<String>();
        int iterations = 1;
        
        while (true) {
            // Cut things off to avoid infinite loop.
            if (iterations > MAX_ITERATIONS) {
                String msg = MsgUtils.getMsg("JOBS_MACRO_TOO_COMPLEX", originalText, MAX_ITERATIONS);
                throw new TapisException(msg);
            }
            
            // Call the recursive method.
            macrosResolved.clear();
            String newText = replaceFirstMacro(text, macrosResolved);
            if (newText == text) break;
            
            // Use the new text on the next iteration. 
            text = newText;
            iterations++;
        }
        
        return text;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* replaceFirstMacro:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Recursive method that replaces the first macro definition in the text with its 
     * value.  The value can itself contain a macro definition, which causes this 
     * method to recursively process the first macro of replacement strings.
     * 
     * Cycles are detected during any recursive call chain, but since this method
     * only replaces the first (leftmost) macro in any string, it is possible for
     * replacement strings containing multiple macro to escape cycle detection.  It
     * is the caller's responsibility to detect such cycles or, at least, limit
     * the number of times it calls this method to avoid infinite loops. 
     * 
     * @param text a string that may contain a macro definition
     * @param macrosResolved the list of macro names already during recursion
     * @return the string with its first macro replaced by its value
     * @throws TapisException if value replacement fails
     */
    private String replaceFirstMacro(String text, ArrayList<String> macrosResolved)
     throws TapisException
    {
        // Find the beginning of the first macro.
        int mstart = text.indexOf(MACRO_DELIMITER);
        if (mstart < 0) return text;
            
        // Find the macro termination.
        int mend = text.substring(mstart).indexOf("}");
        if (mend < 0) {
            String msg = MsgUtils.getMsg("JOBS_MACRO_ILL_FORMED", text);
            throw new TapisException(msg);
        }
        mend += mstart; // make relative to start of text
            
        // Avoid out-of-bounds indexing.
        if (mstart+2 >= mend) return text;
            
        // Split the string into constituent parts.
        String prefix = text.substring(0, mstart);
        String macroName = text.substring(mstart+2, mend);
        String suffix = text.substring(mend+1);
            
        // Detect cycles.
        if (macrosResolved.contains(macroName)) {
            String flatList = String.join(", ", macrosResolved);
            String msg = MsgUtils.getMsg("JOBS_MACRO_CYCLE_DETECTED", text, macroName, flatList);
            throw new TapisException(msg);
        }
            
        // Look up the macro's value.
        String mvalue = _macros.get(macroName);
        if (StringUtils.isBlank(mvalue)) {
            String msg = MsgUtils.getMsg("JOBS_MACRO_MISSING_VALUE", text, macroName);
            throw new TapisException(msg);
        }
            
        // Maybe the returned value contains a macro.
        // Recursively call this method with cycle detection.
        macrosResolved.add(macroName);
        mvalue = replaceFirstMacro(mvalue, macrosResolved);
            
        // Substitute the value in for the macro.
        return prefix + mvalue + suffix;
    }
}

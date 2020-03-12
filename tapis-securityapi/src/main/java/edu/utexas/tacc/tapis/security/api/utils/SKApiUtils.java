package edu.utexas.tacc.tapis.security.api.utils;

import java.util.regex.Pattern;

import javax.ws.rs.core.Response.Status;

import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;

public class SKApiUtils 
{
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Role name validator.  Require names to start with alphabetic characters and 
    // be followed by zero or more alphanumeric characters and underscores.  Note that
    // in particular special characters are disallowed by this regex.
    private static final Pattern _namePattern = Pattern.compile("^\\p{Alpha}(\\p{Alnum}|_)*");
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* toHttpStatus:                                                                */
    /* ---------------------------------------------------------------------------- */
    public static Status toHttpStatus(Condition condition)
    {
        // Conditions are expected to have the exact same names as statuses.
        try {return Status.valueOf(condition.name());}
        catch (Exception e) {return Status.INTERNAL_SERVER_ERROR;}     
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isValidName:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Check a candidate name against the name regex.
     * 
     * @param name the name to validate
     * @return true if matches regex, false otherwise
     */
    public static boolean isValidName(String name)
    {
        if (name == null) return false;
        return _namePattern.matcher(name).matches();
    }
}

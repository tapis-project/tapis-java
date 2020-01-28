package edu.utexas.tacc.tapis.security.api.utils;

import javax.ws.rs.core.Response.Status;

import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;

public class SKApiUtils 
{
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
}

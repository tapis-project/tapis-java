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
        // Map all possible TapisImplException condition codes to http status codes.
        switch (condition) 
        {
            case BAD_REQUEST:           return Status.BAD_REQUEST;
            case INTERNAL_SERVER_ERROR: return Status.INTERNAL_SERVER_ERROR;
            
            default:                    return Status.INTERNAL_SERVER_ERROR;
        }
    }
}

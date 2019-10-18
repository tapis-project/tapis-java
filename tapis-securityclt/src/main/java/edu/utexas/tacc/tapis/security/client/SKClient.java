package edu.utexas.tacc.tapis.security.client;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.gen.model.ResultResourceUrl;
import edu.utexas.tacc.tapis.security.client.gen.model.RespResourceUrl;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class SKClient 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SKClient.class);
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public SKClient(URI uri) {}
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* createRole:                                                                  */
    /* ---------------------------------------------------------------------------- */
    ResultResourceUrl createRole(String roleName, String description)
     throws TapisException
    {
        RespResourceUrl resp = null;
        try {
            
        }
        catch (Exception e) {
            throw new TapisException(e.getMessage(), e);
        }
        
        // 
        if (resp == null) throw new TapisException("No response");
        
        return null;
    }
    
}

package edu.utexas.tacc.tapis.security.commands.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminAbstractProcessor.Op;

public final class SkAdminResults 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminResults.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Singleton instance.
    private static SkAdminResults _instance;
    
    // Summary outcome information.
    private int _secretsCreated;
    private int _secretsUpdated;
    private int _secretsDeployed;
    private int _secretsSkipped;
    private int _secretsFailed;
    
    // Summary generation information.
    private int _keyPairsGenerated;
    private int _passwordsGenerated;
    
    // Detailed information
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private SkAdminResults() {}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** No need for synchronization since SkAdmin is a singled threaded program.
     * 
     * @return the singleton instance of this class
     */
    public static SkAdminResults getInstance()
    {
        if (_instance == null) _instance = new SkAdminResults();
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordSuccess:                                                         */
    /* ---------------------------------------------------------------------- */
    public void recordSuccess(Op op, SecretType type, String message)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordFailure:                                                         */
    /* ---------------------------------------------------------------------- */
    public void recordFailure(Op op, SecretType type, String message)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordSkipped:                                                         */
    /* ---------------------------------------------------------------------- */
    public void recordSkipped(Op op, SecretType type, String message)
    {
        
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
}

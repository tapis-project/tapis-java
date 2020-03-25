package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminDBCredential;

public final class SkAdminDBCredentialProcessor
 extends SkAdminAbstractProcessor<SkAdminDBCredential>
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminDBCredentialProcessor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminDBCredentialProcessor(List<SkAdminDBCredential> secrets, 
                                        SkAdminParameters parms)
    {
        super(secrets, parms);
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* create:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void create(SkAdminDBCredential secret)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* update:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void update(SkAdminDBCredential secret, Op op)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminDBCredential secret)
    {
        
    }    
}

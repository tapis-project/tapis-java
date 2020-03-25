package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminJwtSigning;

public final class SkAdminJwtSigningProcessor
 extends SkAdminAbstractProcessor<SkAdminJwtSigning>
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminJwtSigningProcessor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminJwtSigningProcessor(List<SkAdminJwtSigning> secrets, 
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
    protected void create(SkAdminJwtSigning secret)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* update:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void update(SkAdminJwtSigning secret, Op op)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminJwtSigning secret)
    {
        
    }    
}

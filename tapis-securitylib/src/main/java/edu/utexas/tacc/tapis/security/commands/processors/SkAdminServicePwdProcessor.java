package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminServicePwd;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;

public final class SkAdminServicePwdProcessor
 extends SkAdminAbstractProcessor<SkAdminServicePwd>
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminServicePwdProcessor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminServicePwdProcessor(List<SkAdminServicePwd> secrets, 
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
    protected void create(SkAdminServicePwd secret)
    {
        // See if the secret already exists.
        SkSecret skSecret = null;
        try {
            // First check if the secret already exists.
            var parms = new SKSecretReadParms(SecretType.ServicePwd);
            parms.setTenant(secret.tenant);
            parms.setUser(secret.service);
            parms.setSecretName(secret.secretName);
            skSecret = _skClient.readSecret(parms);
        } 
        catch (TapisClientException e) {
            // Not found is ok.
            if (e.getCode() != 404) {
                // Save the error condition for this secret.
                _results.recordFailure(Op.create, SecretType.ServicePwd, 
                                       makeFailureMessage(Op.create, secret, e.getMessage()));
                return;
            }
        }
        catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(Op.create, SecretType.ServicePwd, 
                                   makeFailureMessage(Op.create, secret, e.getMessage()));
            return;
        }
        
        // Don't overwrite the secret.
        if (skSecret != null) {
            _results.recordSkipped(Op.create, SecretType.ServicePwd, 
                                   makeSkippedMessage(Op.create, secret));
            return;
        }
            
        // Create the secret.
        update(secret, Op.create);
    }
    
    /* ---------------------------------------------------------------------- */
    /* update:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void update(SkAdminServicePwd secret, Op op)
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminServicePwd secret)
    {
        
    }    
    
    /* ---------------------------------------------------------------------- */
    /* makeFailureMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeFailureMessage(Op op, SkAdminServicePwd secret, String errorMsg)
    {
        return " FAILED to " + op.name() + " secret " + secret.secretName +
               " for service " + secret.service + " in tenant " + secret.tenant + 
               ": " + errorMsg;
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSkippedMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSkippedMessage(Op op, SkAdminServicePwd secret)
    {
        return " SKIPPED " + op.name() + " for secret " + secret.secretName +
               " for service " + secret.service + " in tenant " + secret.tenant + 
               ": Already exists.";
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSuccessMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSuccessMessage(Op op, SkAdminServicePwd secret)
    {
        return " SUCCESSFUL " + op.name() + " of secret " + secret.secretName +
               " for service " + secret.service + " in tenant " + secret.tenant + ".";
    }
    
}

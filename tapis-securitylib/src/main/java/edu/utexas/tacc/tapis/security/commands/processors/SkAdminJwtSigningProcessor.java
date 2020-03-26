package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecretMetadata;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminJwtSigning;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminServicePwd;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminAbstractProcessor.Op;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;

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
        // See if the secret already exists.
        SkSecret skSecret = null;
        try {
            // First check if the secret already exists.
            var parms = new SKSecretReadParms(SecretType.JWTSigning);
            parms.setTenant(secret.tenant);
            parms.setUser(secret.user);
            parms.setSecretName(secret.secretName);
            skSecret = _skClient.readSecret(parms);
        } 
        catch (TapisClientException e) {
            // Not found is ok.
            if (e.getCode() != 404) {
                // Save the error condition for this secret.
                _results.recordFailure(Op.create, SecretType.JWTSigning, 
                                       makeFailureMessage(Op.create, secret, e.getMessage()));
                return;
            }
        }
        catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(Op.create, SecretType.JWTSigning, 
                                   makeFailureMessage(Op.create, secret, e.getMessage()));
            return;
        }
        
        // Don't overwrite the secret.  Note that even if "privateKey" isn't
        // present in the existing secret, that fact that the secret exists
        // is enough to cause us to skip the update.  We may in the future
        // want to make this a cumulative secret update operation.
        if (skSecret != null) {
            _results.recordSkipped(Op.create, SecretType.JWTSigning, 
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
    protected void update(SkAdminJwtSigning secret, Op msgOp)
    {
        SkSecretMetadata metadata = null;
        try {
            // Initialize
            var parms = new SKSecretWriteParms(SecretType.JWTSigning);
            parms.setTenant(secret.tenant);
            parms.setUser(secret.user);
            parms.setSecretName(secret.secretName);
            
            // Add the password into the map field. We hardcode
            // the key names in the map that gets saved as the 
            // actual secret map in vault. 
            var map = new HashMap<String,String>();
            map.put(DEFAULT_PRIVATE_KEY_NAME, secret.privateKey);
            if (!StringUtils.isBlank(secret.publicKey))
                map.put(DEFAULT_PUBLIC_KEY_NAME, secret.publicKey);
            parms.setData(map);
            
            // Make the write call.
            metadata = _skClient.writeSecret(parms.getTenant(), parms.getUser(), parms);
        }
        catch (TapisClientException e) {
            // Not found is ok.
            if (e.getCode() != 404) {
                // Save the error condition for this secret.
                _results.recordFailure(msgOp, SecretType.JWTSigning, 
                                       makeFailureMessage(msgOp, secret, e.getMessage()));
                return;
            }
        }
        catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(msgOp, SecretType.JWTSigning, 
                                   makeFailureMessage(msgOp, secret, e.getMessage()));
            return;
        }
        
        // Success.
        _results.recordSuccess(msgOp, SecretType.JWTSigning, 
                               makeSuccessMessage(msgOp, secret));        
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminJwtSigning secret)
    {
        
    }    

    /* ---------------------------------------------------------------------- */
    /* makeFailureMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeFailureMessage(Op op, SkAdminJwtSigning secret, String errorMsg)
    {
        // Set the failed flag to alert any subsequent processing.
        secret.failed = true;
        return " FAILED to " + op.name() + " JWT secret \"" + secret.secretName +
               "\" in tenant \"" + secret.tenant + 
               "\": " + errorMsg;
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSkippedMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSkippedMessage(Op op, SkAdminJwtSigning secret)
    {
        return " SKIPPED " + op.name() + " for JWT secret \"" + secret.secretName +
               "\" in tenant \"" + secret.tenant + 
               "\": Already exists.";
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSuccessMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSuccessMessage(Op op, SkAdminJwtSigning secret)
    {
        return " SUCCESSFUL " + op.name() + " of JWT secret \"" + secret.secretName +
               "\" in tenant " + secret.tenant + "\".";
    }
}
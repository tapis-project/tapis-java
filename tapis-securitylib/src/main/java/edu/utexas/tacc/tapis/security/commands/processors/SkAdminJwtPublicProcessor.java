package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.ISkAdminDeployRecorder;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminJwtPublic;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class processes JWTPublic secrets, which are really a special use case
 * of JWTSigning that allows for public key to be deployed to Kubernetes.  These
 * public keys cannot be created or updated, they only be deployed arbitrary secrets
 * in Kubernetes.  To summarize,
 * 
 *      No secrets are written to SK by this processor, but public keys can be
 *      deployed to Kubernetes.
 * 
 * This processor should always run after the SkAdminJwtSigningProcessor since
 * that processor could create or update the public keys read by this processor.
 * 
 * @author rcardone
 */
public class SkAdminJwtPublicProcessor
 extends SkAdminAbstractProcessor<SkAdminJwtPublic>
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminJwtPublicProcessor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminJwtPublicProcessor(List<SkAdminJwtPublic> secrets, 
                                     SkAdminParameters parms)
    {
        super(secrets, parms);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* create:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void create(SkAdminJwtPublic secret) {}
    
    /* ---------------------------------------------------------------------- */
    /* update:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void update(SkAdminJwtPublic secret, Op msgOp) {}
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminJwtPublic secret, ISkAdminDeployRecorder recorder)
    {
        // Is a public key value already assigned?
        String value = secret.publicKey;
        
        // If necessary try to read the public key from SK.
        if (StringUtils.isBlank(value)) 
        {
            // See if the secret already exists.
            SkSecret skSecret = null;
            try {skSecret = readSecret(secret);} 
            catch (Exception e) {
                // Save the error condition for this secret.
                _results.recordFailure(Op.deploy, SecretType.JWTSigning, 
                                       makeFailureMessage(Op.deploy, secret, e.getMessage()));
                return;
            }
        
            // This shouldn't happen.
            if (skSecret == null || skSecret.getSecretMap().isEmpty()) {
                String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRET_FOUND");
                _results.recordFailure(Op.deploy, SecretType.JWTSigning, 
                                       makeFailureMessage(Op.deploy, secret, msg));
                return;
            }
        
            // Validate the specified secret key's value.
            value = skSecret.getSecretMap().get(DEFAULT_PUBLIC_KEY_NAME);
            if (StringUtils.isBlank(value)) {
                String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRET_FOUND");
                _results.recordFailure(Op.deploy, SecretType.JWTSigning, 
                                       makeFailureMessage(Op.deploy, secret, msg));
                return;
            }
        }
        
        // Record the value as is (no need to base64 encode here).  Use standard key name.
        recorder.addDeployRecord(secret.kubeSecretName, 
              secret.tenant + SkAdminJwtSigningProcessor.PUBLIC_JWT_SIGNING_KUBE_KEY_SUFFIX, 
              value);
    }    

    /* ---------------------------------------------------------------------- */
    /* makeFailureMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String makeFailureMessage(Op op, SkAdminJwtPublic secret, String errorMsg)
    {
        // Set the failed flag to alert any subsequent processing.
        secret.failed = true;
        return " FAILED to " + op.name() + " JWT public key \"" + secret.secretName +
               "\" in tenant \"" + secret.tenant + 
               "\": " + errorMsg;
    }

    // Unused methods.
    @Override
    protected String makeSkippedMessage(Op op, SkAdminJwtPublic secret) {return null;}

    @Override
    protected String makeSuccessMessage(Op op, SkAdminJwtPublic secret) {return null;}

    @Override
    protected String makeSkippedDeployMessage(SkAdminJwtPublic secret) {return null;}
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* readSecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private SkSecret readSecret(SkAdminJwtPublic secret) 
      throws TapisException
    {
        // Try to read a secret.  HTTP 404 is returned if not found.
        var parms = new SKSecretReadParms(SecretType.JWTSigning);
        parms.setTenant(secret.tenant);
        parms.setUser(secret.user);
        parms.setSecretName(secret.secretName);
        return _skClient.readSecret(parms);
    }
}

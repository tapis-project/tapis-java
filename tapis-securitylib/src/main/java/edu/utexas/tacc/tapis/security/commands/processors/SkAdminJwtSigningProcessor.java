package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.Base64;
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
import edu.utexas.tacc.tapis.security.commands.model.ISkAdminDeployRecorder;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminJwtSigning;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SkAdminJwtSigningProcessor
 extends SkAdminAbstractProcessor<SkAdminJwtSigning>
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminJwtSigningProcessor.class);
    
    // Hardcoded public signing key information.  There should be a better way 
    // to do this that doesn't overcomplicate the json input.
    private static final String PUBLIC_JWT_SIGNING_KUBE_KEY_SUFFIX  = "-publickey";
    
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
        try {skSecret = readSecret(secret);} 
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
    protected void deploy(SkAdminJwtSigning secret, ISkAdminDeployRecorder recorder)
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
        String value = skSecret.getSecretMap().get(DEFAULT_PRIVATE_KEY_NAME);
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRET_FOUND");
            _results.recordFailure(Op.deploy, SecretType.JWTSigning, 
                                   makeFailureMessage(Op.deploy, secret, msg));
            return;
        }
        
        // Base64 encode the private key value.
        String base64Value = Base64.getEncoder().encodeToString(value.getBytes());
        recorder.addDeployRecord(secret.kubeSecretName, secret.kubeSecretKey, base64Value);
        
        // Automatic public key publishing if the key is present.
        value = skSecret.getSecretMap().get(DEFAULT_PUBLIC_KEY_NAME);
        if (StringUtils.isBlank(value)) return;
        
        // Base64 encode the public key value.
        base64Value = Base64.getEncoder().encodeToString(value.getBytes());
        recorder.addDeployRecord(_parms.kubeJWTSigningPublicKeySecret, 
                                 secret.tenant + PUBLIC_JWT_SIGNING_KUBE_KEY_SUFFIX,
                                 base64Value);
    }    

    /* ---------------------------------------------------------------------- */
    /* readSecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private SkSecret readSecret(SkAdminJwtSigning secret) 
      throws TapisException
    {
     // Try to read a secret.  HTTP 404 is returned if not found.
        var parms = new SKSecretReadParms(SecretType.JWTSigning);
        parms.setTenant(secret.tenant);
        parms.setUser(secret.user);
        parms.setSecretName(secret.secretName);
        return _skClient.readSecret(parms);
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

package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.VaultImpl;
import edu.utexas.tacc.tapis.security.authz.model.SkSecret;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.ISkAdminDeployRecorder;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminJwtPublic;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SkAdminVaultJwtPublicProcessor
 extends SkAdminJwtPublicProcessor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminVaultJwtPublicProcessor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminVaultJwtPublicProcessor(List<SkAdminJwtPublic> secrets, 
                                          SkAdminParameters parms)
    {
        super(secrets, parms);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminJwtPublic secret, ISkAdminDeployRecorder recorder)
    {
        // Is a public key value already assigned?
        String value = secret.publicKey;
        
        // If necessary try to read the public key from Vault.
        if (StringUtils.isBlank(value)) 
        {
            // See if the secret already exists.
            SkSecret skSecret = null;
            try {skSecret = readSecret(secret);} 
            catch (Exception e) {
                // Save the error condition for this secret.
                _results.recordFailure(Op.deploy, secret.getClientSecretType(), 
                                       makeFailureMessage(Op.deploy, secret, e.getMessage()));
                return;
            }
        
            // This shouldn't happen.
            if (skSecret == null || skSecret.secretMap.isEmpty()) {
                String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRET_FOUND");
                _results.recordFailure(Op.deploy, secret.getClientSecretType(), 
                                      makeFailureMessage(Op.deploy, secret, msg));
                return;
            }
        
            // Validate the specified secret key's value.
            value = skSecret.secretMap.get(DEFAULT_PUBLIC_KEY_NAME);
            if (StringUtils.isBlank(value)) {
                String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRET_FOUND");
                _results.recordFailure(Op.deploy, secret.getClientSecretType(), 
                                       makeFailureMessage(Op.deploy, secret, msg));
                return;
            }
        }
        
        // Record the value as is (no need to base64 encode here).  Use standard key name.
        recorder.addDeployRecord(secret.kubeSecretName, 
              secret.tenant + SkAdminJwtSigningProcessor.PUBLIC_JWT_SIGNING_KUBE_KEY_SUFFIX, 
              value);
    }    

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* readSecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private SkSecret readSecret(SkAdminJwtPublic secret)
     throws Exception, TapisImplException
    {
        // Null response means the secret type and its required parameters are present.
        SecretPathMapperParms secretPathParms = secret.getSecretPathParms();
        return VaultImpl.getInstance().secretRead(secret.tenant, secret.user, secretPathParms, 
                                                  DEFAULT_SECRET_VERSION);
    }
}

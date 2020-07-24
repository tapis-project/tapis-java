package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.impl.VaultImpl;
import edu.utexas.tacc.tapis.security.authz.model.SkSecret;
import edu.utexas.tacc.tapis.security.authz.model.SkSecretMetadata;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.ISkAdminDeployRecorder;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminServicePwd;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SkAdminVaultServicePwdProcessor
 extends SkAdminServicePwdProcessor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminVaultServicePwdProcessor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminVaultServicePwdProcessor(List<SkAdminServicePwd> secrets, 
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
    protected void create(SkAdminServicePwd secret)
    {
        // See if the secret already exists.
        SkSecret skSecret = null;
        try {skSecret = readSecret(secret);}
        catch (TapisImplException e) {
            if (e.condition != Condition.NOT_FOUND) {
                // Save the error condition for this secret.
                _results.recordFailure(Op.create, secret.getClientSecretType(), 
                                       makeFailureMessage(Op.create, secret, e.getMessage()));
                return;
            }
        } catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(Op.create, secret.getClientSecretType(), 
                                   makeFailureMessage(Op.create, secret, e.getMessage()));
            return;
        }

        // Don't overwrite the secret.  Note that even if "password" isn't
        // present in the existing secret, that fact that the secret exists
        // is enough to cause us to skip the update.  We may in the future
        // want to make this a cumulative secret update operation.
        if (skSecret != null) {
            _results.recordSkipped(Op.create, secret.getClientSecretType(), 
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
    protected void update(SkAdminServicePwd secret, Op msgOp)
    {
        // ------------------------- Path Processing --------------------------
        // Null response means the secret type and its required parameters are present.
        SecretPathMapperParms secretPathParms;
        try {secretPathParms = secret.getSecretPathParms();}
        catch (Exception e) {
            _log.error(e.getMessage(), e);
            // Save the error condition for this secret.
            _results.recordFailure(msgOp, secret.getClientSecretType(), 
                                   makeFailureMessage(msgOp, secret, e.getMessage()));
          return;
        }
        
        // Get the secret's key/value pair(s). 
        var secretMap = secret.getSecretMap();
        
        // ------------------------ Request Processing ------------------------
        SkSecretMetadata metadata = null;
        try {
            metadata = VaultImpl.getInstance().secretWrite(secret.tenant, secret.user, 
                                                           secretPathParms, secretMap);
        }
        catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(msgOp, secret.getClientSecretType(), 
                                   makeFailureMessage(msgOp, secret, e.getMessage()));
            return;
        }
        
        // Success.
        _results.recordSuccess(msgOp, secret.getClientSecretType(), 
                               makeSuccessMessage(msgOp, secret));        
    }

    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminServicePwd secret, ISkAdminDeployRecorder recorder)
    {
        // Is this secret slated for deployment?
        if (StringUtils.isBlank(secret.kubeSecretName)) {
            _results.recordDeploySkipped(makeSkippedDeployMessage(secret));
            return;
        }    
        
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
        String value = skSecret.secretMap.get(DEFAULT_KEY_NAME);
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRET_FOUND");
            _results.recordFailure(Op.deploy, secret.getClientSecretType(), 
                                   makeFailureMessage(Op.deploy, secret, msg));
            return;
        }
        
        // Record the value as is (no need to base64 encode here).
        recorder.addDeployRecord(secret.kubeSecretName, secret.kubeSecretKey, value);
    }    
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* readSecret:                                                            */
    /* ---------------------------------------------------------------------- */
    private SkSecret readSecret(SkAdminServicePwd secret)
     throws Exception, TapisImplException
    {
        // Null response means the secret type and its required parameters are present.
        SecretPathMapperParms secretPathParms = secret.getSecretPathParms();
        return VaultImpl.getInstance().secretRead(secret.tenant, secret.user, secretPathParms, 
                                                  DEFAULT_SECRET_VERSION);
    }
}

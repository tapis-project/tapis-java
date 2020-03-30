package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecretMetadata;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminDBCredential;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;

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
        // See if the secret already exists.
        SkSecret skSecret = null;
        try {
            // First check if the secret already exists.
            var parms = new SKSecretReadParms(SecretType.DBCredential);
            parms.setTenant(secret.tenant);
            parms.setUser(secret.user);
            parms.setDbHost(secret.dbhost);
            parms.setDbName(secret.dbname);
            parms.setDbService(secret.dbservice);
            parms.setSecretName(secret.secretName);
            skSecret = _skClient.readSecret(parms);
        } 
        catch (TapisClientException e) {
            // Not found is ok.
            if (e.getCode() != 404) {
                // Save the error condition for this secret.
                _results.recordFailure(Op.create, SecretType.DBCredential, 
                                       makeFailureMessage(Op.create, secret, e.getMessage()));
                return;
            }
        }
        catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(Op.create, SecretType.DBCredential, 
                                   makeFailureMessage(Op.create, secret, e.getMessage()));
            return;
        }
        
        // Don't overwrite the secret.  Note that even if "password" isn't
        // present in the existing secret, that fact that the secret exists
        // is enough to cause us to skip the update.  We may in the future
        // want to make this a cumulative secret update operation.
        if (skSecret != null) {
            _results.recordSkipped(Op.create, SecretType.DBCredential, 
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
    protected void update(SkAdminDBCredential secret, Op msgOp)
    {
        SkSecretMetadata metadata = null;
        try {
            // Initialize
            var parms = new SKSecretWriteParms(SecretType.DBCredential);
            parms.setTenant(secret.tenant);
            parms.setUser(secret.user);
            parms.setDbHost(secret.dbhost);
            parms.setDbName(secret.dbname);
            parms.setDbService(secret.dbservice);
            parms.setSecretName(secret.secretName);
            
            // Add the password into the map field. We hardcode
            // the key as "password" in a single element map that
            // gets saved as the actual secret map in vault. 
            var map = new HashMap<String,String>();
            map.put(DEFAULT_KEY_NAME, secret.secret);
            parms.setData(map);
            
            // Make the write call.
            metadata = _skClient.writeSecret(parms.getTenant(), parms.getUser(), parms);
        }
        catch (TapisClientException e) {
            // Not found is ok.
            if (e.getCode() != 404) {
                // Save the error condition for this secret.
                _results.recordFailure(msgOp, SecretType.DBCredential, 
                                       makeFailureMessage(msgOp, secret, e.getMessage()));
                return;
            }
        }
        catch (Exception e) {
            // Save the error condition for this secret.
            _results.recordFailure(msgOp, SecretType.DBCredential, 
                                   makeFailureMessage(msgOp, secret, e.getMessage()));
            return;
        }
        
        // Success.
        _results.recordSuccess(msgOp, SecretType.DBCredential, 
                               makeSuccessMessage(msgOp, secret));        
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    protected void deploy(SkAdminDBCredential secret)
    {
        
    }    

    /* ---------------------------------------------------------------------- */
    /* makeFailureMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeFailureMessage(Op op, SkAdminDBCredential secret, String errorMsg)
    {
        // Set the failed flag to alert any subsequent processing.
        secret.failed = true;
        return " FAILED to " + op.name() + " secret \"" + secret.secretName +
               "\" for service \"" + secret.dbservice + "\" on dbhost \"" + secret.dbhost +
               "\" in db \"" + secret.dbname + "\" for dbuser \"" + secret.user +
               "\": " + errorMsg;
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSkippedMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSkippedMessage(Op op, SkAdminDBCredential secret)
    {
        return " SKIPPED " + op.name() + " for secret \"" + secret.secretName +
               "\" for service \"" + secret.dbservice + "\" on dbhost \"" + secret.dbhost +
               "\" in db \"" + secret.dbname + "\" for dbuser \"" + secret.user +
               "\": Already exists.";
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSuccessMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    private String makeSuccessMessage(Op op, SkAdminDBCredential secret)
    {
        return " SUCCESSFUL " + op.name() + " of secret \"" + secret.secretName +
                "\" for service \"" + secret.dbservice + "\" on dbhost \"" + secret.dbhost +
                "\" in db \"" + secret.dbname + "\" for dbuser \"" + secret.user +
                "\".";
    }
}

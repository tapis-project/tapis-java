package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTester;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection.AuthMethod;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public abstract class AbsTester 
 implements RecoverTester
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbsTester.class);
    
    // Batch size of recovered jobs.
    protected static final int NO_RESUBMIT_BATCHSIZE = 0;
    protected static final int DEFAULT_RESUBMIT_BATCHSIZE = 10;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // The recovery job to which this policy will be applied.
    protected final JobRecovery _jobRecovery;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected AbsTester( JobRecovery jobRecovery)
    {
        _jobRecovery = jobRecovery;
    }

    /* **************************************************************************** */
    /*                              Protected Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getSystem:                                                                   */
    /* ---------------------------------------------------------------------------- */
    protected TapisSystem getSystem(String username, String tenant, String systemId,
                                    AuthnMethod authnMethod) 
     throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the systems client.
        SystemsClient client = 
            ServiceClients.getInstance().getClient(username, tenant, SystemsClient.class);
            
        // Lookup the system.
        Boolean returnCreds = Boolean.TRUE;
        Boolean requireExecPerm = Boolean.FALSE;
        return client.getSystem(systemId, returnCreds, authnMethod, requireExecPerm);
    }

    /* ---------------------------------------------------------------------- */
    /* getSSHConnection:                                                      */
    /* ---------------------------------------------------------------------- */
    protected SSHConnection getSSHConnection(String username, String systemId, 
                                             AuthMethod authMethod) 
     throws JobRecoveryAbortException
    {
        // We currently support two authentication methods.
        AuthnMethod authnMethod = null;
        if (authMethod == AuthMethod.PASSWORD_AUTH) authnMethod = AuthnMethod.PASSWORD;
        else if (authMethod == AuthMethod.PUBLICKEY_AUTH) authnMethod = AuthnMethod.PKI_KEYS;
        else {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_UNKNOWN_SSH_AUTHN", authMethod.name());
            throw new JobRecoveryAbortException(msg);
        }
        
        // Get the system definition with credentials.
        TapisSystem system = null;
        try {system = getSystem(username, _jobRecovery.getTenantId(), systemId, authnMethod);}
            catch (TapisRecoverableException e) {
                // Record problem.
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_TEST_SETUP_ERROR", 
                                             _jobRecovery.getId(), e.getMessage());
                _log.warn(msg);
                return null;  // Try again later.
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg(e.getMessage());
                throw new JobRecoveryAbortException(msg, e);
            }
        
        // Get the credentials.
        var cred = system.getAuthnCredential();
        if (cred == null) {
            String msg = MsgUtils.getMsg("SYSTEMS_MISSING_CREDENTIALS", 
                                         getSystemHostMessage(system), 
                                         system.getTenant());
            throw new JobRecoveryAbortException(msg);
        }

        // Try to connect to the system.  Note that we use the host, port
        // and username from system even though they are also in the tester
        // parameters.  They should be the same, but ones in the system are
        // the ones that will be used during job processing.
        SSHConnection conn = null;
        try {
            if (system.getDefaultAuthnMethod() == AuthnEnum.PASSWORD) 
                conn = new SSHConnection(system.getHost(), system.getPort(), 
                                     system.getEffectiveUserId(), cred.getPassword());
            else 
                conn = new SSHConnection(system.getHost(), system.getEffectiveUserId(), 
                                     system.getPort(), cred.getPublicKey(), cred.getPrivateKey());
        } catch (TapisRecoverableException e) {
            // The error condition has not cleared,
            // but we live to fight another day.
            return null;
        } catch (Exception e) {
            String msg = MsgUtils.getMsg(e.getMessage());
            throw new JobRecoveryAbortException(msg, e);
        }
        
        // Successful connection.
        return conn;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSystemHostMessage:                                                  */
    /* ---------------------------------------------------------------------- */
    protected String getSystemHostMessage(TapisSystem system)
    {return system.getId() + " (" + system.getHost() + ")";}
}

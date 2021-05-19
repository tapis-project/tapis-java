package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;

import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SSHConnectionTester 
 extends SSHAuthenticationTester
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SSHConnectionTester.class);
    
    // The tester parameter key used by this tester.
    public static final String PARM_CHANNEL_TYPE = "channelType";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Set to true the first time the tester parameters are validated.
    private boolean _parmsValidated = false;
    
    // Validated test parameters saved as fields for convenient access.
    private String            _channelType;
    
    /* ********************************************************************** */
    /*                               Constructors                             */
    /* ********************************************************************** */
    /* -----------------------------------------------------------------------*/
    /* constructor:                                                           */
    /* -----------------------------------------------------------------------*/
    public SSHConnectionTester(JobRecovery jobRecovery)
    {
        super(jobRecovery);
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* canUnblock:                                                            */
    /* ---------------------------------------------------------------------- */
    @Override
    public int canUnblock(Map<String, String> testerParameters) 
     throws JobRecoveryAbortException
    {
        // Validate the tester parameters. 
        // An exception can be thrown here.
        validateTesterParameters(testerParameters);
        
        // Try to connect.
        boolean canConnectChannel = canEstablishConnection();
        
        // Return whether is the system is marked available.
        if (canConnectChannel) return DEFAULT_RESUBMIT_BATCHSIZE;
        else return NO_RESUBMIT_BATCHSIZE;
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* canEstablishConnection:                                                */
    /* ---------------------------------------------------------------------- */
    protected boolean canEstablishConnection() throws JobRecoveryAbortException
    {
        // Recovery abort exception can be thrown from here.
        var conn = getSSHConnection(_username, _systemId, _authMethod);
        if (conn == null) return false;
        
        // Try to connect on a channel.
        Channel channel = null;
        try {channel = conn.createChannel(_channelType);}
            catch (TapisRecoverableException e) {
                // Falling through allows more retries.
            } 
            catch (TapisException e) {
                // Failing in some unrecoverable way.
                throw new JobRecoveryAbortException(e.getMessage(), e);
            }
        
        // What happened?
        boolean channelConnected = (channel == null) ? false : true;
        
        // Clean up.
        if (channelConnected) conn.returnChannel(channel);
        conn.closeSession();
        
        // A connected channel means the condition has cleared.
        if (channelConnected) return true;
         else return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateTesterParameters:                                              */
    /* ---------------------------------------------------------------------- */
    /** Validate the tester parameters for existence and type. If validation 
     * fails on the first attempt, then the exception thrown will cause the
     * recovery abort and all its jobs to be failed. If validation success
     * on the first attempt, then we skip validation in this object from now on.
     * 
     * @param testerParameters the test parameter map
     * @throws JobRecoveryAbortException on validation error
     */
    protected void validateTesterParameters(Map<String, String> testerParameters)
     throws JobRecoveryAbortException
    {
        // No need to validate more than once.
        if (_parmsValidated) return;
        
        // Validate superclass.
        super.validateTesterParameters(testerParameters);
        
        // Validate our specialized fields.
        _channelType = testerParameters.get(PARM_CHANNEL_TYPE);
        if (StringUtils.isBlank(_channelType)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                          PARM_CHANNEL_TYPE);
            _log.error(msg);
            throw new JobRecoveryAbortException(msg);
        }
        
        // We're good if we get here.
        _parmsValidated = true;
    }
}

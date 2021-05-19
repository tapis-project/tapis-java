package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection.AuthMethod;

public class SSHAuthenticationTester 
 extends AbsTester
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SSHAuthenticationTester.class);
    
    // The tester parameter key used by this tester.
    public static final String PARM_SYSTEM_ID = "systemId";
    public static final String PARM_AUTH_METHOD = "authMethod";
    public static final String PARM_HOSTNAME = "hostname";
    public static final String PARM_PORT = "port";
    public static final String PARM_PROXY_HOST = "proxyHost";
    public static final String PARM_PROXY_PORT = "proxyPort";
    public static final String PARM_USERNAME = "username";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Set to true the first time the tester parameters are validated.
    private boolean _parmsValidated = false;
    
    // Validated test parameters saved as fields for convenient access.
    protected String            _systemId;
    protected AuthMethod        _authMethod;
    protected String            _username;
    protected String            _hostname;
    protected int               _port;
    
    /* ********************************************************************** */
    /*                               Constructors                             */
    /* ********************************************************************** */
    /* -----------------------------------------------------------------------*/
    /* constructor:                                                           */
    /* -----------------------------------------------------------------------*/
    public SSHAuthenticationTester(JobRecovery jobRecovery)
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
        boolean canConnect = canEstablishConnection();
        
        // Return whether is the system is marked available.
        if (canConnect) return DEFAULT_RESUBMIT_BATCHSIZE;
        else return NO_RESUBMIT_BATCHSIZE;
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* canEstablishConnection:                                                */
    /* ---------------------------------------------------------------------- */
    protected boolean canEstablishConnection() throws JobRecoveryAbortException
    {
        // Recovery abort exception can be thrown from here.
        var conn = getSSHConnection(_username, _systemId, _authMethod);
        if (conn == null) return false;
        
        // An established connection means the condition has cleared.
        conn.closeSession();
        return true;
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
        
        // ---- The protocol type must be present.
        String s = testerParameters.get(PARM_AUTH_METHOD);
        if (StringUtils.isBlank(s)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                         PARM_AUTH_METHOD);
            _log.error(msg);
            throw new JobRecoveryAbortException(msg);
        }
        
        // The protocol type must be valid.
        try {_authMethod = AuthMethod.valueOf(s);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INVALID_TESTER_TYPE",
                                             _jobRecovery.getId(), s);
                _log.error(msg, e);
                throw new JobRecoveryAbortException(msg, e);
            }
        
        // ---- Make sure all the other required parms are acceptable.
        _systemId = testerParameters.get(PARM_SYSTEM_ID);
        if (StringUtils.isBlank(_systemId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                         PARM_SYSTEM_ID);
            _log.error(msg);
            throw new JobRecoveryAbortException(msg);
        }
        _username = testerParameters.get(PARM_USERNAME);
        if (StringUtils.isBlank(_username)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                         PARM_USERNAME);
            _log.error(msg);
            throw new JobRecoveryAbortException(msg);
        }
        _hostname = testerParameters.get(PARM_HOSTNAME);
        if (StringUtils.isBlank(_hostname)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                         PARM_HOSTNAME);
            _log.error(msg);
            throw new JobRecoveryAbortException(msg);
        }
        
        // We don't use the port, but we make sure it's set.
        String port = testerParameters.get(PARM_PORT);
        if (StringUtils.isBlank(port)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                         PARM_PORT);
            _log.error(msg);
            throw new JobRecoveryAbortException(msg);
        }
        try {_port = Integer.valueOf(port);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateTesterParameters",
                                             PARM_PORT, port);
                _log.error(msg, e);
                throw new JobRecoveryAbortException(msg, e);
            }
        
        // We're good if we get here.
        _parmsValidated = true;
    }
}

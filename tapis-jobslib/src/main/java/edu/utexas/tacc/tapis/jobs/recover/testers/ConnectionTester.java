package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;

public final class ConnectionTester 
 extends AbsTester
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ConnectionTester.class);
    
    // The tester parameter key used by this tester.
    public static final String PARM_LOGIN_PROTOCOL_TYPE = "loginProtocolType";
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
//    private LoginProtocolType _protocol;
    private String            _username;
    private String            _hostname;
    private int               _port;
    private String            _proxyHost;
    private int               _proxyPort;
    
    /* ********************************************************************** */
    /*                               Constructors                             */
    /* ********************************************************************** */
    /* -----------------------------------------------------------------------*/
    /* constructor:                                                           */
    /* -----------------------------------------------------------------------*/
    public ConnectionTester(JobRecovery jobRecovery)
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
        // TODO:
        return NO_RESUBMIT_BATCHSIZE;
        
//        // Validate the tester parameters. 
//        // An exception can be thrown here.
//        validateTesterParameters(testerParameters);
//        
//        // Create the SSH remote client. We don't need a password just to connect.
//        // The proxy host and port can also be null.
//        MaverickSSHSubmissionClient client = 
//            new MaverickSSHSubmissionClient(
//                    _hostname, 
//                    _port, 
//                    _username, 
//                    null, 
//                    _proxyHost, 
//                    _proxyPort);
//        
//        // See if we can connect without authentication to the host.
//        // The connection is always close by the called method.
//        boolean available = client.canEstablishSSHConnection();
//        
//        // Return whether is the system is marked available.
//        if (available) return DEFAULT_RESUBMIT_BATCHSIZE;
//        else return NO_RESUBMIT_BATCHSIZE;
    }
    
//    /* ---------------------------------------------------------------------- */
//    /* validateTesterParameters:                                              */
//    /* ---------------------------------------------------------------------- */
//    /** Validate the tester parameters for existence and type. If validation 
//     * fails on the first attempt, then the exception thrown will cause the
//     * recovery abort and all its jobs to be failed. If validation success
//     * on the first attempt, then we skip validation in this object from now on.
//     * 
//     * @param testerParameters the test parameter map
//     * @throws JobRecoveryAbortException on validation error
//     */
//    private void validateTesterParameters(Map<String, String> testerParameters)
//     throws JobRecoveryAbortException
//    {
//        // No need to validate more than once.
//        if (_parmsValidated) return;
//        
//        // ---- The protocol type must be present.
//        String s = testerParameters.get(PARM_LOGIN_PROTOCOL_TYPE);
//        if (s == null) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_INVALID_TESTER_TYPE",
//                                         _jobRecovery.getId(), "null");
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        
//        // The protocol type must be valid.
//        try {_protocol = LoginProtocolType.valueOf(s);}
//            catch (Exception e) {
//                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INVALID_TESTER_TYPE",
//                                             _jobRecovery.getId(), s);
//                _log.error(msg, e);
//                throw new JobRecoveryAbortException(msg, e);
//            }
//        
//        // We currently only handle SSH connections.
//        if (_protocol != LoginProtocolType.SSH) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_INVALID_TESTER_TYPE",
//                    _jobRecovery.getId(), _protocol.name());
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        
//        // ---- Make sure all the other required parms are acceptable.
//        _username = testerParameters.get(PARM_USERNAME);
//        if (StringUtils.isBlank(_username)) {
//            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "validateTesterParameters", 
//                                         PARM_USERNAME);
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        _hostname = testerParameters.get(PARM_HOSTNAME);
//        if (StringUtils.isBlank(_hostname)) {
//            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "validateTesterParameters", 
//                                         PARM_HOSTNAME);
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        if (StringUtils.isBlank(testerParameters.get(PARM_PORT))) {
//            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "validateTesterParameters", 
//                                         PARM_PORT);
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        
//        // ---- Optional parameter can be null;
//        _proxyHost = testerParameters.get(PARM_PROXY_HOST);
//        
//        // Numeric conversion tests.
//        try {_port = Integer.valueOf(testerParameters.get(PARM_PORT));}
//            catch (Exception e) {
//                String msg = MsgUtils.getMsg("ALOE_INVALID_PARAMETER", "validateTesterParameters", 
//                                             PARM_PORT, testerParameters.get(PARM_PORT));
//                _log.error(msg, e);
//                throw new JobRecoveryAbortException(msg, e);
//            }
//        if (!StringUtils.isBlank(testerParameters.get(PARM_PROXY_PORT)))
//            try {_proxyPort = Integer.valueOf(testerParameters.get(PARM_PROXY_PORT));}
//                catch (Exception e) {
//                    String msg = MsgUtils.getMsg("ALOE_INVALID_PARAMETER", "validateTesterParameters", 
//                                                 PARM_PROXY_PORT, testerParameters.get(PARM_PROXY_PORT));
//                    _log.error(msg, e);
//                    throw new JobRecoveryAbortException(msg, e);
//                }
//        
//        // We're good if we get here.
//        _parmsValidated = true;
//    }
}

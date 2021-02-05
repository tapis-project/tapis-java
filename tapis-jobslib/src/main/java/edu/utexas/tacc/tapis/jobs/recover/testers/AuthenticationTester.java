package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;

public final class AuthenticationTester 
 extends AbsTester
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AuthenticationTester.class);
    
    // The tester parameter keys used by this tester.
    public static final String PARM_LOGIN_PROTOCOL_TYPE = "loginProtocolType";
    public static final String PARM_ID = "id";
    public static final String PARM_SYSTEM_ID = "systemId";
    public static final String PARM_SYSTEM_TYPE = "systemType";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Set to true the first time the tester parameters are validated.
    private boolean _parmsValidated = false;
    
    // Validated test parameters saved as fields for convenient access.
    // Note that additional test parameter collected in the transport 
    // layer are not explicitly accessed here, but their existence does 
    // affect the equivalence class into which a job is placed.
//    private LoginProtocolType _protocol;    // authentication protocol
    private long              _id;          // db primary key
    private String            _systemId;    // sysytem's text id
//    private RemoteSystemType  _systemType;  // system's type
    
    
    /* ********************************************************************** */
    /*                               Constructors                             */
    /* ********************************************************************** */
    /* -----------------------------------------------------------------------*/
    /* constructor:                                                           */
    /* -----------------------------------------------------------------------*/
    public AuthenticationTester(JobRecovery jobRecovery)
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
//        // Get the system dao.
//        SystemDao dao = null;
//        try {dao = new SystemDao(JobDao.getDataSource());}
//        catch (Exception e) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_ACCESS", _jobRecovery.getId(), 
//                                         "SystemDao", e.getMessage());
//            _log.error(msg, e);
//            return NO_RESUBMIT_BATCHSIZE;
//        }
//        
//        // Determine the type of system to which we are authenticating.
//        boolean available;
//        if (_systemType == RemoteSystemType.EXECUTION) 
//            available = canUnblockExecuteSystem(dao);
//          else available = canUnblockStorageSystem(dao); 
//        
//        // Return whether is the system is marked available.
//        if (available) return DEFAULT_RESUBMIT_BATCHSIZE;
//        else return NO_RESUBMIT_BATCHSIZE;
    }
    
//    /* ---------------------------------------------------------------------- */
//    /* canUnblockExecuteSystem:                                               */
//    /* ---------------------------------------------------------------------- */
//    private boolean canUnblockExecuteSystem(SystemDao dao) 
//     throws JobRecoveryAbortException 
//    {
//        // Retrieve the system definition.
//        ExecutionSystem execSystem = null;
//        try {execSystem = dao.getExecutionSystemById(_jobRecovery.getTenantId(), _id);}
//        catch (Exception e) {
//            // Ignore database errors so that we can retry recovery later. 
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_ACCESS", _jobRecovery.getId(), 
//                                         "SystemDao", e.getMessage());
//            _log.error(msg, e);
//            return false;
//        }
//        
//        // If the system's not found, we abandon recovery for associated jobs.
//        if (execSystem == null) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_EXEC_SYSTEM_NOT_FOUND", _jobRecovery.getId(),
//                                         _systemId, _jobRecovery.getTenantId());
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        
//        // Get the remote data client factory.
//        RemoteSubmissionClientFactory factory = null;
//        try {factory = RemoteSubmissionClientFactory.getFactory(JobDao.getDataSource());}
//        catch (Exception e) {
//            // Retrieving the data source should not fail since we did it above and it's cached. 
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_ACCESS", _jobRecovery.getId(), 
//                                         "JobDao", e.getMessage());
//            _log.error(msg, e);
//            return false;
//        }
//        
//        // Get a remote data client for the system.
//        RemoteSubmissionClient client = null;
//        try {client = factory.getInstance(execSystem);}
//        catch (Exception e) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_GET_CLIENT", _jobRecovery.getId(), 
//                                         execSystem.getSystemId(), _jobRecovery.getTenantId(),
//                                         e.getMessage());
//            _log.error(msg, e);
//            return false;
//        }
//        
//        // See if we can authenticate to the host.
//        // The connection is always closed by the called method.
//        return client.canAuthenticate();
//    }
//    
//    /* ---------------------------------------------------------------------- */
//    /* canUnblockStorageSystem:                                               */
//    /* ---------------------------------------------------------------------- */
//    private boolean canUnblockStorageSystem(SystemDao dao) 
//     throws JobRecoveryAbortException 
//    {
//        // Retrieve the system definition.
//        StorageSystem storageSystem = null;
//        try {storageSystem = dao.getStorageSystemById(_jobRecovery.getTenantId(), _id);}
//        catch (Exception e) {
//            // Ignore database errors so that we can retry recovery later. 
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_ACCESS", _jobRecovery.getId(), 
//                                         "SystemDao", e.getMessage());
//            _log.error(msg, e);
//            return false;
//        }
//        
//        // If the system's not found, we abandon recovery for associated jobs.
//        if (storageSystem == null) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_STORAGE_SYSTEM_NOT_FOUND", _jobRecovery.getId(),
//                                         _systemId, _jobRecovery.getTenantId());
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        
//        // Get the remote data client factory.
//        RemoteDataClientFactory factory = null;
//        try {factory = RemoteDataClientFactory.getInstance(JobDao.getDataSource());}
//        catch (Exception e) {
//            // Retrieving the data source should not fail since we did it above and it's cached. 
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_ACCESS", _jobRecovery.getId(), 
//                                         "JobDao", e.getMessage());
//            _log.error(msg, e);
//            return false;
//        }
//        
//        // Get a remote data client for the system.
//        RemoteDataClient client = null;
//        try {client = factory.getRemoteDataClient(storageSystem);}
//        catch (Exception e) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_GET_CLIENT", _jobRecovery.getId(), 
//                                         storageSystem.getSystemId(), _jobRecovery.getTenantId(),
//                                         e.getMessage());
//            _log.error(msg, e);
//            return false;
//        }
//        
//        // Be optimistic.
//        boolean available = true;
//        try {client.authenticate();}
//            catch (Exception e) {available = false;}
//            finally {client.disconnect();} // always disconnect
//        
//        return available;
//    }
//    
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
//                                         _jobRecovery.getId(), _protocol.name());
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        
//        // ---- The remote system id must be present.
//        s = testerParameters.get(PARM_SYSTEM_ID);
//        if (StringUtils.isBlank(s)) {
//            String msg = MsgUtils.getMsg("JOBS_RECOVERY_NO_SYSTEM_ID", _jobRecovery.getId());
//            _log.error(msg);
//            throw new JobRecoveryAbortException(msg);
//        }
//        _systemId = s;
//        
//        // ---- The database id must be present.
//        try {_id = Long.valueOf(testerParameters.get(PARM_ID));}
//        catch (Exception e) {
//            String msg = MsgUtils.getMsg("ALOE_INVALID_PARAMETER", "validateTesterParameters", 
//                                         PARM_ID, testerParameters.get(PARM_ID));
//            _log.error(msg, e);
//            throw new JobRecoveryAbortException(msg, e);
//        }
//        
//        // ---- The protocol type must be present.
//        try {_systemType = RemoteSystemType.valueOf(testerParameters.get(PARM_SYSTEM_TYPE));}
//        catch (Exception e) {
//            String msg = MsgUtils.getMsg("ALOE_INVALID_PARAMETER", "validateTesterParameters", 
//                                         PARM_SYSTEM_TYPE, testerParameters.get(PARM_SYSTEM_TYPE));
//            _log.error(msg, e);
//            throw new JobRecoveryAbortException(msg, e);
//        }
//
//        // We're good if we get here.
//        _parmsValidated = true;
//    }
}

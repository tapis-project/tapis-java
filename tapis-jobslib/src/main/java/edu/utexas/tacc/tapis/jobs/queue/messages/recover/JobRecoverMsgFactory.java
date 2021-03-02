package edu.utexas.tacc.tapis.jobs.queue.messages.recover;

import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.recover.RecoverConditionCode;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTesterType;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobRecoverMsgFactory 
{
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobRecoverMsgFactory.class);
    
    // Pre-defined recovery configurations.
    public enum RecoveryConfiguration {
        DFT_SYSTEM_NOT_AVAILABLE,
        DFT_APPLICATION_NOT_AVAILABLE,
        DFT_SERVICE_CONNECTION_FAILURE,
        DFT_CONNECTION_FAILURE,
        DFT_QUOTA_EXCEEDED,
        DFT_AUTHENTICATION_FAILURE,
        FIRST_AUTHENTICATION_FAILURE
    }
    
    // TODO: Implement more recoverable configurations.
    //       DFT_TRANSMISSION_FAILURE, DFT_DATABASE_NOT_AVAILABLE, 
    //       DFT_QUEUE_BROKER_NOT_AVAILABLE, DFT_JOB_SUSPENDED

    /* ---------------------------------------------------------------------- */
    /* getJobRecoverMsg:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Convenience method for when the caller doesn't include any parameter
     * maps.
     * 
     * @param config the configuration type
     * @param job the job that needs to be recovered
     * @param senderId the sending code or component
     * @param msg a explanatory message
     * @return a recovery message to be queue on the recovery queue
     */
    public static JobRecoverMsg getJobRecoverMsg(RecoveryConfiguration config, 
                                                 Job job, String senderId, String msg)
    {
        return getJobRecoverMsg(config, job, senderId, msg, null, null);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobRecoverMsg:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Method that creates a recovery message based on the condition that
     * blocked the job.
     * 
     * @param config the configuration type
     * @param job the job that needs to be recovered
     * @param senderId the sending code or component
     * @param msg a explanatory message
     * @param policyParameters extra policy data passed to the recovery mananger
     * @param testerParameters extra tester data passed to the recovery mananger
     * @return a recovery message to be queue on the recovery queue
     */
    public static JobRecoverMsg getJobRecoverMsg(RecoveryConfiguration config, 
                                                 Job job, String senderId, String msg,
                                                 TreeMap<String,String> policyParameters,
                                                 TreeMap<String,String> testerParameters)
    {
        // The config parameter cannot be null.
        if (config == null) {
          String emsg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "getJobRecoverMsg", "config");
          _log.error(emsg);
          throw new TapisRuntimeException(emsg);
        }
        
        // Create the recovery message with the specified configuration.
        JobRecoverMsg rmsg = null;
        switch (config)
        {
            case DFT_SYSTEM_NOT_AVAILABLE:
                rmsg = JobRecoverMsg.create(
                        job, 
                        senderId, 
                        RecoverConditionCode.SYSTEM_NOT_AVAILABLE, 
                        RecoverPolicyType.CONSTANT_BACKOFF, 
                        policyParameters, 
                        job.getStatus(), 
                        msg, 
                        RecoverTesterType.DEFAULT_SYSTEM_AVAILABLE_TESTER, 
                        testerParameters);
                break;
                
            case DFT_APPLICATION_NOT_AVAILABLE:
                rmsg = JobRecoverMsg.create(
                        job, 
                        senderId, 
                        RecoverConditionCode.APPLICATION_NOT_AVAILABLE, 
                        RecoverPolicyType.CONSTANT_BACKOFF, 
                        policyParameters, 
                        job.getStatus(), 
                        msg, 
                        RecoverTesterType.DEFAULT_APPLICATION_TESTER, 
                        testerParameters);
                break;
                
            case DFT_SERVICE_CONNECTION_FAILURE:
                rmsg = JobRecoverMsg.create(
                        job, 
                        senderId, 
                        RecoverConditionCode.SERVICE_CONNECTION_FAILURE, 
                        RecoverPolicyType.STEPWISE_BACKOFF, 
                        policyParameters, 
                        job.getStatus(), 
                        msg, 
                        RecoverTesterType.DEFAULT_SERVICE_CONNECTION_TESTER, 
                        testerParameters);
                break;
                
            case DFT_CONNECTION_FAILURE:
                rmsg = JobRecoverMsg.create(
                        job, 
                        senderId, 
                        RecoverConditionCode.CONNECTION_FAILURE, 
                        RecoverPolicyType.STEPWISE_BACKOFF, 
                        policyParameters, 
                        job.getStatus(), 
                        msg, 
                        RecoverTesterType.DEFAULT_CONNECTION_TESTER, 
                        testerParameters);
                break;
                
            case DFT_QUOTA_EXCEEDED:
                rmsg = JobRecoverMsg.create(
                        job, 
                        senderId, 
                        RecoverConditionCode.QUOTA_EXCEEDED, 
                        RecoverPolicyType.CONSTANT_BACKOFF, 
                        policyParameters, 
                        job.getStatus(), 
                        msg, 
                        RecoverTesterType.DEFAULT_QUOTA_TESTER, 
                        testerParameters);
                break;
                
            case DFT_AUTHENTICATION_FAILURE:
                rmsg = JobRecoverMsg.create(
                        job, 
                        senderId, 
                        RecoverConditionCode.AUTHENTICATION_FAILED, 
                        RecoverPolicyType.CONSTANT_BACKOFF, 
                        policyParameters, 
                        job.getStatus(), 
                        msg, 
                        RecoverTesterType.DEFAULT_AUTHENTICATION_TESTER, 
                        testerParameters);
                break;

            case FIRST_AUTHENTICATION_FAILURE:
                rmsg = JobRecoverMsg.create(
                        job,
                        senderId,
                        RecoverConditionCode.FIRST_AUTHENTICATION_FAILED,
                        RecoverPolicyType.CONSTANT_BACKOFF,
                        policyParameters,
                        job.getStatus(),
                        msg,
                        RecoverTesterType.DEFAULT_AUTHENTICATION_TESTER,
                        testerParameters);
                break;

            default:
                String emsg = MsgUtils.getMsg("JOBS_UNKNOWN_ENUM", "RecoveryConfiguration",
                                              config, job.getUuid());
                _log.error(emsg);
                throw new TapisRuntimeException(emsg);
        }
        
        return rmsg;
    }
}

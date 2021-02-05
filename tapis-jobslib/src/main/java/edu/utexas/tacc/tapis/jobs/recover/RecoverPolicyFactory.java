package edu.utexas.tacc.tapis.jobs.recover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.policies.ConstantBackoffPolicy;
import edu.utexas.tacc.tapis.jobs.recover.policies.StepwiseBackoffPolicy;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Singleton factory class that returns recovery policies.
 * 
 * @author rcardone
 */
public class RecoverPolicyFactory 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoverPolicyFactory.class);

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    private static RecoverPolicyFactory _instance;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private RecoverPolicyFactory() {}
    
    /* **************************************************************************** */
    /*                                 Public Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getInstance:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public static RecoverPolicyFactory getInstance()
    {
        // Serialize instance creation without incurring
        // synchronization overhead most of the time.
        if (_instance == null) {
            synchronized (RecoverPolicyFactory.class) {
                if (_instance == null) _instance = new RecoverPolicyFactory(); 
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getPolicy:                                                                   */
    /* ---------------------------------------------------------------------------- */
    public RecoverPolicy getPolicy(JobRecovery jobRecovery)
     throws TapisRuntimeException
    {
        // Create the specified policy object.
        RecoverPolicyType policyType = jobRecovery.getPolicyType();
        switch (policyType) {
            case STEPWISE_BACKOFF:
                return new StepwiseBackoffPolicy(jobRecovery);
            case CONSTANT_BACKOFF:
                return new ConstantBackoffPolicy(jobRecovery);
            default:
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_UNKNOWN_POLICYTYPE",  
                                             policyType, jobRecovery.getId(),
                                             jobRecovery.getTenantId());
                _log.error(msg);
                throw new TapisRuntimeException(msg);
        }
    }
}

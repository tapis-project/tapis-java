package edu.utexas.tacc.tapis.jobs.recover.policies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class is a container for parameters for all the ConstantBackoff 
 * policy class.  
 * 
 * @author rcardone
 */
public final class ConstantBackoffPolicyParameters
 extends AbsPolicyParameters
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ConstantBackoffPolicyParameters.class);
    
    // Minimum wait time is 1 minute.
    public static final long MIN_WAIT_MS = 60000; 
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // ----------------- ConstantBackoff parameters -----------------
    // The constant backoff policy returns the same millisecond wait time on every
    // query unless one of the limits has been exceeded.
    private long _waitTime = MIN_WAIT_MS;
    
    // The constant backoff policy can be configured to limit the number
    // of tests it performs.  If zero or less, the maximum number of tries
    // (max int) will be allowed.
    private int _maxTries;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public ConstantBackoffPolicyParameters(JobRecovery jobRecovery) 
    {
        super(RecoverPolicyType.CONSTANT_BACKOFF, jobRecovery);
        initialize();
    }

    /* **************************************************************************** */
    /*                                Private Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* initialize:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Replace default field values with user-specified values if they exist. */
    private void initialize()
    {
        // -- Assign the milliseconds between test attempts.
        String value = _jobRecovery.getPolicyParameters().get("waitTime");
        if (value != null) {
            try {_waitTime = Long.valueOf(value);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("JOB_RECOVERY_BAD_POLICY_PARM", 
                                                 "waitTime", value, e.getMessage());
                    _log.error(msg, e);
                }
        }
        
        // Make sure the wait time does get set below our minimum.
        _waitTime = Math.max(MIN_WAIT_MS, _waitTime);
        
        // -- Assign the maximum number of test attempts.
        value = _jobRecovery.getPolicyParameters().get("maxTries");
        if (value != null) {
            try {_maxTries = Integer.valueOf(value);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("JOB_RECOVERY_BAD_POLICY_PARM", 
                                                 "maxTries", value, e.getMessage());
                    _log.error(msg, e);
                }
        }

        // Canonicalize maximum tries.
        if (_maxTries <= 0) _maxTries = Integer.MAX_VALUE;
    }
    
    /* **************************************************************************** */
    /*                                 Public Methods                               */
    /* **************************************************************************** */
    public int getMaxTries() {
        return _maxTries;
    }

    public void setMaxTries(int maxTries) {
        this._maxTries = maxTries;
    }

    public long getWaitTime() {
        return _waitTime;
    }

    public void setWaitTime(long waitTime) {
        this._waitTime = waitTime;
    }
}

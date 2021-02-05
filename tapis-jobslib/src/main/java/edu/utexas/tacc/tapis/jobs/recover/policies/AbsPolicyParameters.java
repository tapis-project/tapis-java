package edu.utexas.tacc.tapis.jobs.recover.policies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public abstract class AbsPolicyParameters 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbsPolicyParameters.class);
    
    // We keep retrying for 48 hours by default.
    public static final long DEFAULT_MAX_ELAPSED_SECONDS = 48 * 60 * 60;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // The type of the policy associated with the concrete subclass.
    protected final RecoverPolicyType _policyType;
    
    // The recovery job to which this policy will be applied.
    protected final JobRecovery       _jobRecovery;
    
    // This parameter limits the maximum number of seconds between the
    // first try and the last try.  This value provides a way to 
    // cap the amount of real time that the policy executes no matter
    // how the steps are configured.  
    //
    // A value of zero or less disables this parameter.
    protected long                    _maxElapsedSeconds = DEFAULT_MAX_ELAPSED_SECONDS;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public AbsPolicyParameters(RecoverPolicyType policyType, JobRecovery jobRecovery)
    {
        this._policyType  = policyType;
        this._jobRecovery = jobRecovery;
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
        // -- Assign maxElapsedSeconds
        String value = _jobRecovery.getPolicyParameters().get("maxElapsedSeconds");
        if (value != null) {
            try {_maxElapsedSeconds = Long.valueOf(value);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("JOB_RECOVERY_BAD_POLICY_PARM", 
                                                 "maxElapsedSeconds", value, e.getMessage());
                    _log.error(msg);
                }
        }
    }
    
    /* **************************************************************************** */
    /*                                 Public Methods                               */
    /* **************************************************************************** */
    public long getMaxElapsedSeconds() {
        return _maxElapsedSeconds;
    }

    public void setMaxElapsedSeconds(long maxElapsedSeconds) {
        this._maxElapsedSeconds = maxElapsedSeconds;
    }

    public RecoverPolicyType getPolicyType() {
        return _policyType;
    }

    public JobRecovery getJobRecovery() {
        return _jobRecovery;
    }
}

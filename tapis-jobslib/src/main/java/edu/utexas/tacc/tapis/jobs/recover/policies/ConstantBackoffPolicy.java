package edu.utexas.tacc.tapis.jobs.recover.policies;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicy;

public final class ConstantBackoffPolicy
 implements RecoverPolicy
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    private static final Logger _log = LoggerFactory.getLogger(ConstantBackoffPolicy.class);
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The pointer back to the owning recovery job.
    private final JobRecovery        _jobRecovery;
    
    // Parameters specific to this policy type.
    private final ConstantBackoffPolicyParameters _parms;
    
    // The time of the first try and last allowed try.  
    // Initialized by initFields() on first millisToWait call.
    private Instant                  _firstTry;
    private Instant                  _lastTry;
    private int                      _tryCount;
    
    // The reason why the last call to millisToWait returned null.
    private ReasonCode               _reasonCode;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ConstantBackoffPolicy(JobRecovery jobRecovery) 
    {
        _jobRecovery = jobRecovery;
        _parms = new ConstantBackoffPolicyParameters(jobRecovery);
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* millisToWait:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Return the number of milliseconds to wait as long as we haven't hit
     * either the maximum time or maximum attempts limit. If a limit has been
     * exceeded, return null.
     * 
     * Whenever null is return, the reason code field MUST be set to a non-null
     * value. 
     * 
     * @return the number of milliseconds or null
     */
    @Override
    public Long millisToWait() 
    {
        // Set the maximum elapsed time the first time this method is called.
        Instant now = Instant.now();
        if (_firstTry == null) initFields(now);
        
        // See if the maximum elapsed time has been exceeded.
        if (_lastTry.isBefore(now)) {
            _reasonCode = ReasonCode.TIME_EXPIRED;
            return null;
        }
        
        // Increment the attempt counter and see if we've 
        // exceeded our maximum number of attempts.
        if (++_tryCount > _parms.getMaxTries()) {
            _reasonCode = ReasonCode.TOO_MANY_ATTEMPTS;
            return null;
        }
        
        // Return the constant wait time.
        return _parms.getWaitTime();
    }

    /* ---------------------------------------------------------------------- */
    /* getReasonCode:                                                         */
    /* ---------------------------------------------------------------------- */
    @Override
    public ReasonCode getReasonCode() {return _reasonCode;}
    
    /* **************************************************************************** */
    /*                                Private Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initFields:                                                            */
    /* ---------------------------------------------------------------------- */
    /** One time initialization when on first millisToWait call. */
    private void initFields(Instant now)
    {
        // Set the elapsed time fields.
        _firstTry = now;
        if (_parms.getMaxElapsedSeconds() <= 0) _lastTry = Instant.MAX;
         else _lastTry  = _firstTry.plusSeconds(_parms.getMaxElapsedSeconds()); 
        
        // Get the current number of times this test has been attempted.
        _tryCount = _jobRecovery.getNumAttempts();
    }
}

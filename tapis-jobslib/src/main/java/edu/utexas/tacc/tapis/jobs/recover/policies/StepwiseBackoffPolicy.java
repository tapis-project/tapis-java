package edu.utexas.tacc.tapis.jobs.recover.policies;

import java.time.Instant;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicy;

public final class StepwiseBackoffPolicy
 implements RecoverPolicy
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    private static final Logger _log = LoggerFactory.getLogger(StepwiseBackoffPolicy.class);
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The pointer back to the owning recovery job.
    private final JobRecovery                     _jobRecovery;
    
    // Parameters specific to this policy type.
    private final StepwiseBackoffPolicyParameters _parms;
    
    // The steps to try represented as <numTries, millisecond delay> tuples.
    private final List<Pair<Integer,Long>>        _steps;
    
    // The time of the first try and last allowed try.  
    // Initialized by initFields() on first millisToWait call.
    private Instant                               _firstTry;
    private Instant                               _lastTry;
    private Pair<Integer,Long>                    _curStep;
    private int                                   _curStepIndex;
    private int                                   _curStepTryCount;
    
    // The reason why the last call to millisToWait returned null.
    private ReasonCode                            _reasonCode;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public StepwiseBackoffPolicy(JobRecovery jobRecovery) 
    {
        _jobRecovery = jobRecovery;
        _parms = new StepwiseBackoffPolicyParameters(jobRecovery);
        _steps = _parms.getSteps();
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* millisToWait:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Return the number of milliseconds to wait based on the step was are
     * currently in.  Return null if the policy has expired because all steps
     * have completed or because the overall time limit has been exceeded.
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
        
        // Get the maximum tries for this step and
        // handle the infinite try case.
        int maxTries = _curStep.getLeft();
        if (maxTries <= 0) return _curStep.getRight();
        
        // Have we consumed this step?
        if (_curStepTryCount > maxTries) {
            // We may have processed all our steps.
            if (++_curStepIndex >= _steps.size()) {
                _reasonCode = ReasonCode.TOO_MANY_ATTEMPTS;
                return null;
            }
            
            // Move to the next step.
            _curStep = _steps.get(_curStepIndex);
            _curStepTryCount = 0;
        }
        
        // Consume the next try on the current step.
        _curStepTryCount++;
        return _curStep.getRight();
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
    /** One time initialization when on first millisToWait call.
     */
    private void initFields(Instant now)
    {
        // Set the elapsed time fields.
        _firstTry = now;
        if (_parms.getMaxElapsedSeconds() <= 0) _lastTry = Instant.MAX;
         else _lastTry  = _firstTry.plusSeconds(_parms.getMaxElapsedSeconds()); 
        
        // Initialize the step controls to the first step on
        // on newly monitored jobs or where we left off on 
        // partially monitored jobs.
        initStepSettings();
    }
    
    /* ---------------------------------------------------------------------- */
    /* initStepSettings:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Pick up where monitoring left off before monitoring for this job was 
     * interrupted or, if this job has never been monitored before, start at 
     * the beginning of the first step.
     */
    private void initStepSettings()
    {
        // Get the total number of attempts to date.
        long attempts = _jobRecovery.getNumAttempts();
        
        // Initialize the local stepIndex and tryCount.
        int stepIndex = 0;
        int stepTries = 0;
        
        // Walk through the steps until all attempts are consumed.
        // If this is a new monitor policy, we skip the whole loop.
        while (attempts > 0) {
            // Replay the attempts in each step until either the step
            // is consumed or the number of previous attempts is consumed.
            stepTries = _steps.get(stepIndex).getKey();
            while (stepTries > 0 && attempts > 0) {stepTries--; attempts--;}
            
            // If we've depleted the attempts, then the current step and its
            // try count are where we want to pick up from.
            if (attempts <= 0) break;
            
            // We shouldn't run out of steps, but we still protect ourselves. 
            if (++stepIndex >= _steps.size()) break;
        }
        
        // The last calculated step index and try number 
        // within that step is where we pick up monitoring.
        _curStepIndex = stepIndex;
        _curStep = _steps.get(stepIndex);
        _curStepTryCount = stepTries;
    }
}

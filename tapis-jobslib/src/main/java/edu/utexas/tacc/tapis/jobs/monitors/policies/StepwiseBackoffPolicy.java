package edu.utexas.tacc.tapis.jobs.monitors.policies;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class StepwiseBackoffPolicy 
 implements MonitorPolicy
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    private static final Logger _log = LoggerFactory.getLogger(StepwiseBackoffPolicy.class);
    
    // Extend the monitor timeout so that the remote scheduler times out first.
    private static final int MONITOR_TIMEOUT_EXTENSION_SECS = 600; // 10 minutes
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private final Job _job;
    
    // The steps to try represented as <numTries, millisecond delay> tuples.
    private List<Pair<Integer,Long>> _steps;
    private long                     _maxElapsedSeconds;
    private long                     _maxConsecutiveFailureMinutes;
    
    // Flags that record policy initialization status.
    private boolean                  _fieldsInitialized;
    private boolean                  _runningTimeInitialized;
    
    // The time of the first try and last allowed try.
    private Instant                  _monitorStart;
    private Instant                  _runStartTime;
    private Instant                  _runEndTime;
    private Instant                  _firstFailureInSeries;
    
    private Pair<Integer,Long>       _curStep;
    private int                      _curStepIndex;
    private int                      _curStepTryCount;
    private long                     _stepConnectionCloseMillis;
    
    // The reason why the last call to millisToWait returned null.
    private ReasonCode               _reasonCode;
    
    // For now we limit the initial queuing race condition
    // retries by accepting the default maximum.
    private int                      _initialQueuingRetries = MAX_INITIAL_QUEUED_RETRIES;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public StepwiseBackoffPolicy(Job job, MonitorPolicyParameters policyParameters) 
     throws JobException 
    {
        // Check input.
        if (job == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "StepwiseBackoff", "job");
            _log.error(msg);
            throw new JobException(msg);
        }
        if (policyParameters == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "StepwiseBackoff", "policyParameters");
            _log.error(msg);
            throw new JobException(msg);
        }
        
        _job = job;
        setParameters(policyParameters);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* millisToWait:                                                          */
    /* ---------------------------------------------------------------------- */
    @Override
    public Long millisToWait(boolean lastAttemptFailed) 
    {
        // Set the maximum elapsed time the first time this method is called.
        if (!_fieldsInitialized) initFields();
        
        // Determine if we've had too many failed attempts.
        Instant now = Instant.now();
        if (tooManyFailures(lastAttemptFailed, now)) {
            _reasonCode = ReasonCode.TOO_MANY_FAILURES;
            return null;
        }
        
        // See if the maximum elapsed time has been exceeded.
        if (_runningTimeInitialized && _runEndTime.isBefore(now)) {
            _reasonCode = ReasonCode.TIME_EXPIRED;
            return null;
        }
        
        // Get the maximum tries for this step and
        // handle the infinite try case.
        int maxTries = _curStep.getLeft();
        if (maxTries <= 0) return _curStep.getRight();
        
        // Have we consumed this step?
        if (_curStepTryCount >= maxTries) {
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
    /* keepConnection:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Get the policy cutoff for connection closing.  If the time until the 
     * next scheduled status query is less than the configured amount, than the
     * policy recommends keeping the connection open.  Otherwise, the monitoring
     * code should close the connection to release resources at both ends.
     * 
     * @return true if the policy recommends keeping the connection open, 
     *         false otherwise
     */
    @Override
    public boolean keepConnection()
    {
        // This call should not be made before the first millisToWait call,
        // but if it is we're protected.
        if (_curStep == null) return false; 
        return _curStep.getRight().longValue() < _stepConnectionCloseMillis;
    }
    
    /* ---------------------------------------------------------------------- */
    /* retryForInitialQueuing:                                                */
    /* ---------------------------------------------------------------------- */
    /** This method should only be called when the first remote query returns
     * a null or empty response.  See the interface definition for details.
     */
    @Override
    public boolean retryForInitialQueuing()
    {
        // This only applies if the job was just queued.
        if (_job.getStatus() != JobStatusType.QUEUED) return false;
        
        // This method should not be called before initialization, 
        // i.e., before millisToWait has been called at least once.
        if (_curStep == null) return false;
        
        // Determine if this profile has only been called once.
        if (_curStepIndex != 0 || _curStepTryCount > 1) return false;
        
        // We only retry for the initial queuing problem a finite number of times.
        if (_initialQueuingRetries <= 0) return false;
        
        // Sleep for the configured initial queue condition time.
        try {Thread.sleep(INITIAL_QUEUED_MILLIS);} 
            catch (InterruptedException e) {
                if (_log.isDebugEnabled()) {
                    String msg = MsgUtils.getMsg("JOBS_MONITOR_INTERRUPTED", _job.getUuid(), 
                                                 getClass().getSimpleName());
                    _log.debug(msg);
                }
            }
        _initialQueuingRetries--; // decrement the retry counter.
        
        // Let's start over. 
        _curStepTryCount = 0;     
        
        // Tell the caller that they should retry the remote query.
        return true;
    }
    

    /* ---------------------------------------------------------------------- */
    /* getReasonCode:                                                         */
    /* ---------------------------------------------------------------------- */
    /** The reason code is null until the no more monitoring attempts should
     * be made (i.e., millisToWait returns null).
     * 
     * @return the reason code for canceling monitoring or null if monitoring
     *         is not cancelled
     */
    @Override
    public ReasonCode getReasonCode() {return _reasonCode;}
    
    
    /* ---------------------------------------------------------------------- */
    /* startJobExecutionTimer:                                                */
    /* ---------------------------------------------------------------------- */
    /** Allow callers to start the execution timer when a job transition from
     * QUEUED to RUNNING. If the job starts out in RUNNING, the timer fields
     * are automatically initialized.
     */
    @Override
    public void startJobExecutionTimer()
    {
        // Only initialize the running time clock once.
        if (!_runningTimeInitialized) initRunningTimeSettings();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* tooManyFailures:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Determine if we've had a run of failed monitoring attempts that 
     * exceed the configured allowed maximum.
     * 
     * @param lastAttemptFailed true if the last attempt failed
     * @param now the current time
     * @return true if we've experienced failures for too long, false otherwise
     */
    private boolean tooManyFailures(boolean lastAttemptFailed, Instant now)
    {
        // Maybe there's nothing to worry about.
        if (!lastAttemptFailed) {
            _firstFailureInSeries = null;
            return false;
        }
        
        // Is this failure the beginning of a new failure series?
        if (_firstFailureInSeries == null) _firstFailureInSeries = now;
           
        // Determine if the duration of a series of consecutive
        // failures has exceeded its time limit.
        if (_firstFailureInSeries.plus(_maxConsecutiveFailureMinutes, ChronoUnit.MINUTES).isBefore(now))
            return true;
        
        // We're still ok.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setParameters:                                                         */
    /* ---------------------------------------------------------------------- */
    private void setParameters(MonitorPolicyParameters policyParameters)
    {
        // Set the steps.
        if (policyParameters.steps == null || policyParameters.steps.isEmpty())
            _steps = getDefaultSteps();
          else _steps = policyParameters.steps;
        
        // Set the maximum elapsed time.
        if (policyParameters.maxElapsedSeconds <= 0) 
            _maxElapsedSeconds = getDefaultMaxElapsedSeconds();
         else _maxElapsedSeconds = policyParameters.maxElapsedSeconds;
        
        // Step the connection close cutoff that maintains connections
        // to remote systems when the next status check might happen in
        // less then the configured number of milliseconds.
        _stepConnectionCloseMillis = policyParameters.stepConnectionCloseMillis;
        
        // Get the maximum number of minutes to sustain failed request attempts.
        _maxConsecutiveFailureMinutes = policyParameters.maxConsecutiveFailureMinutes;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDefaultSteps:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Default step configuration that should work for most uses.
     * 
     * @return the step list
     */
    private List<Pair<Integer,Long>> getDefaultSteps()
    {
        // Each step specifies a number of tries with the given delay in milliseconds.
        // Depending on the stepConnectionCloseMillis parameter (default is 6 minutes),
        // connections are closed only if the next wait time is greater than the 
        // parameter value.
        ArrayList<Pair<Integer,Long>> steps = new ArrayList<>();
        steps.add(Pair.of(1,   1000L));   // 1 second 
        steps.add(Pair.of(5,   10000L));  // 10 seconds 
        steps.add(Pair.of(10,  60000L));  // 1 minute 
        steps.add(Pair.of(100, 180000L)); // 3 minutes 
        steps.add(Pair.of(100, 300000L)); // 5 minutes 
        steps.add(Pair.of(-1,  600000L)); // 10 minutes forever
        
        return steps;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getDefaultMaxElapsedSeconds:                                           */
    /* ---------------------------------------------------------------------- */
    /** Get the maximum runtime from the job.  
     * 
     * Note that the job field must be set by the time this method is called.
     * 
     * @return the default maximum runtime in seconds 
     */
    private long getDefaultMaxElapsedSeconds()
    {
        // We expect the job field to be set.
        long maxSeconds = _job.getMaxMinutes() * 60;
        if (maxSeconds <= 0) maxSeconds = (long) (Job.DEFAULT_MAX_MINUTES * 60);
        
        // If the job is going to be submitted to a remote scheduler like slurm,
        // we add a little more time to our local limit to allow the remote 
        // scheduler to time out first.  This defers job management to the 
        // scheduler that is better placed to handle things.
        if (hasRemoteScheduler()) maxSeconds += MONITOR_TIMEOUT_EXTENSION_SECS;
        
        return maxSeconds;
    }
    
    /* ---------------------------------------------------------------------- */
    /* initFields:                                                            */
    /* ---------------------------------------------------------------------- */
    /** One time initialization when on first millisToWait call.
     */
    private void initFields()
    {
        // Set the start time of monitoring.
        _monitorStart = Instant.now();
        
        // Set the elapsed time fields if the job is already executing.
        if (_job.getStatus() == JobStatusType.RUNNING) 
            initRunningTimeSettings(_monitorStart);
        
        // Initialize the step controls to the first step on
        // on newly monitored jobs or where we left off on 
        // partially monitored jobs.
        initStepSettings();
        
        // Indicate field initialization complete.
        _fieldsInitialized = true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* initRunningTimeSettings:                                               */
    /* ---------------------------------------------------------------------- */
    /** Perform the one time initialization of the execution timer parameters.
     * If no parameter is provide, the current time is taken to be the start time.
     * 
     * @param now optional begin time
     */
    private void initRunningTimeSettings(Instant... now)
    {
        // Set the elapsed time fields.
        _runStartTime = now.length > 0 ? now[0] : Instant.now();
        _runEndTime  = _runStartTime.plusSeconds(_maxElapsedSeconds);
        
        // Mark execution times as initialized.
        _runningTimeInitialized = true;
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
        long attempts = _job.getRemoteChecksSuccess() + _job.getRemoteChecksFailed();
        
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
            if ((stepIndex + 1) >= _steps.size()) break;
            stepIndex++; // Only increment the index up to step size minus 1.
        }
        
        // The last calculated step index and try number 
        // within that step is where we pick up monitoring.
        _curStepIndex = stepIndex;
        _curStep = _steps.get(stepIndex);
        _curStepTryCount = stepTries;
    }
    
    /* ---------------------------------------------------------------------- */
    /* hasRemoteScheduler:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Determine if the execution uses a scheduler.
     * 
     * @return true if a scheduler is used, false for FORK systems
     */
    private boolean hasRemoteScheduler()
    {
        // We probably have the execution system object cached in the context.
        JobExecutionContext jobCtx = _job.getJobCtx();
        if (jobCtx == null) {
            // This should never happen.
            _log.warn(MsgUtils.getMsg("JOBS_NO_CONTEXT", _job.getUuid()));
            return false;
        }
        
        // See if the execution system specifies a remote scheduler.
        try {
            if (jobCtx.getJob().getJobType() == JobType.BATCH) return true;;
        }
        catch (Exception e) {
            _log.error(MsgUtils.getMsg("JOBS_EXEC_SYSTEM_RETRIEVAL_ERROR", _job.getExecSystemId(),
                                       _job.getTenant(), _job.getUuid(), e));
        }
        
        // Exceptions and fork jobs fall through.
        return false;
    }
}

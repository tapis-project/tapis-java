package edu.utexas.tacc.tapis.jobs.recover;

import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.utexas.tacc.tapis.apps.client.gen.model.App;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobAppAvailableException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoverableException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoveryDefinitions;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoveryDefinitions.BlockedJobActivity;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobSSHAuthException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobSSHConnectionException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobSystemAvailableException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsgFactory;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsgFactory.RecoveryConfiguration;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisAppAvailableException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisQuotaException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHConnectionException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSystemAvailableException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public final class RecoveryUtils 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoveryUtils.class);
    
    // Constants.
    public static final Integer DEFAULT_MAX_SYSTEM_JOBS          = Integer.MAX_VALUE;
    public static final Integer DEFAULT_MAX_SYSTEM_JOBS_PER_USER = Integer.MAX_VALUE;
    public static final long    DEFAULT_MAX_JOBS                 = Long.MAX_VALUE;
    public static final long    DEFAULT_MAX_USER_JOBS            = Long.MAX_VALUE;
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* makeJobRecoverableException:                                           */
    /* ---------------------------------------------------------------------- */
    /** Search the exception call chain for a trigger exception, which is an 
     * exception that indicates a recoverable error has occurred.  Subtypes
     * of JobRecoverableException are themselves trigger exceptions when they
     * are the top-level exception passed into this method.  The other trigger
     * exceptions are found in the edu.utexas.tacc.tapis.shared.exceptions.recoverable
     * package in the aloe-shared project. These exceptions will be discovered
     * if they appear anywhere in the chain of causal exceptions starting with
     * the top-level exception passed into this method.  
     * 
     * @param e the top-level exception whose causal chain may be searched
     * @param jobCtx the context of the job that experienced the problem
     * @return a recoverable exception or null if no recovery should be attempted
     */
    public static JobRecoverableException makeJobRecoverableException(
                                    TapisException e, JobExecutionContext jobCtx) 
    {
        // There's nothing to do if we already have a recoverable exception.
        // Note that recoverable exceptions should always be top-level; they
        // should never be wrapped in other exceptions.  See JobUtils.aloeify()
        // for details.
        if (e instanceof JobRecoverableException) return (JobRecoverableException)e;
        
        // See if there's a recoverable exception in the cause chain.  We search
        // for each trigger type separately and, when one is found, it is wrapped
        // in a new recoverable exception that gets thrown.
        Exception trigger;
        trigger = TapisUtils.findInChain(e, TapisSSHConnectionException.class);
        if (trigger != null) return handleTrigger(e, jobCtx, (TapisSSHConnectionException)trigger);

        trigger = TapisUtils.findInChain(e, TapisSSHAuthException.class);
        if (trigger != null) return handleTrigger(e, jobCtx, (TapisSSHAuthException)trigger);

        trigger = TapisUtils.findInChain(e, TapisSystemAvailableException.class);
        if (trigger != null) return handleTrigger(e, jobCtx, (TapisSystemAvailableException)trigger);
        
        trigger = TapisUtils.findInChain(e, TapisAppAvailableException.class);
        if (trigger != null) return handleTrigger(e, jobCtx, (TapisAppAvailableException)trigger);
        
        trigger = TapisUtils.findInChain(e, TapisQuotaException.class);
        if (trigger != null) return handleTrigger(e, jobCtx, (TapisQuotaException)trigger);
        
        // We found no trigger exception from which we can recover, 
        // so we return null.
        return null;
    }
    
    /* ---------------------------------------------------------------------- */
    /* captureSystemState:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Capture the state of an unavailable system for which recovery will be
     * attempted.  The job-specific information is automatically captured when 
     * the JobRecoveryMsg is constructed. 
     * 
     * General Guidance
     * ----------------
     * The tester parameters are converted to json and then hashed to provide
     * equivalence classes for jobs in recovery (see JobRecoverMsg.setTesterInfo()
     * for details).  Tester parameters should not contain extra values not used 
     * in testing that unnecessarily distinguish the hashes.  The goal is to 
     * coalesce all jobs blocked on the same condition to be serviced by the same
     * recovery record.  
     * 
     * @param system the execution or storage system that is not currently available
     * @return the recovery information
     */
    public static TreeMap<String,String> captureSystemState(TSystem system)
    {
        TreeMap<String,String> state = new TreeMap<>();
        state.put("id", Long.toString(system.getSeqId()));
        state.put("name", system.getId());
        state.put("tenantId", system.getTenant());
        return state;
    }
    
    /* ---------------------------------------------------------------------- */
    /* captureAppState:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Capture the state of an unavailable application for which recovery will be
     * attempted.  The job-specific information is automatically captured when 
     * the JobRecoveryMsg is constructed. 
     * 
     * General Guidance
     * ----------------
     * The tester parameters are converted to json and then hashed to provide
     * equivalence classes for jobs in recovery (see JobRecoverMsg.setTesterInfo()
     * for details).  Tester parameters should not contain extra values not used 
     * in testing that unnecessarily distinguish the hashes.  The goal is to 
     * coalesce all jobs blocked on the same condition to be serviced by the same
     * recovery record.  
     * 
     * Note that some application parameters are included to improve debugging
     * and are not used in the actual testing.  These extra parameters have no
     * significant effect since they are tied to the specific application id 
     * (i.e., the equivalence classes are not disturbed).
     * 
     * @param app the application that is not currently available
     * @return the recovery information
     */
    public static TreeMap<String,String> captureAppState(App app)
    {
        TreeMap<String,String> state = new TreeMap<>();
        state.put("id", Long.toString(app.getSeqId()));
        state.put("name", app.getId());
        state.put("version", app.getVersion());
        state.put("tenantId", app.getTenant());
        return state;
    }
    
    /* ---------------------------------------------------------------------- */
    /* captureQuotaState:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Capture the information need to check all aloe enforced job quotas.
     * If values are missing, we use defaults that cause quota checks to be
     * skipped so that all keys are always assigned a non-null value. 
     * 
     * The numeric values are saved here so that recovery code does not have to
     * query the database for quota values.  This approach trades off dynamic
     * quotas for faster processing in almost all cases.  Specifically, if an
     * execution system's quotas change after a job is in recovery, the job
     * will still use the old quota values during recovery testing.  On the plus
     * side, the execution system and batchqueue definitions do not have to be
     * loaded during recovery.  The only database calls that must be issued
     * during recovery are those that query the jobs table to determine if
     * quotas are still exceeded. 
     * 
     * General Guidance
     * ----------------
     * The tester parameters are converted to json and then hashed to provide
     * equivalence classes for jobs in recovery (see JobRecoverMsg.setTesterInfo()
     * for details).  Tester parameters should not contain extra values not used 
     * in testing that unnecessarily distinguish the hashes.  The goal is to 
     * coalesce all jobs blocked on the same condition to be serviced by the same
     * recovery record. 
     * 
     * Note that there's a certain amount of imprecision in this implementation
     * because we lump system quotas violations with user and queue violations.
     * A finer grained implementation would separate system-only violations from
     * those that are user or queue specific.  The net result is that more testing
     * takes place than is absolutely necessary.
     * 
     * @param execSys the execution system
     * @param job the job that needs recovery
     * @return the recovery information 
     */
    public static TreeMap<String,String> captureQuotaState(TSystem execSys,
                                                           Job job, 
                                                           LogicalQueue remoteQueue)
    {
        TreeMap<String,String> state = new TreeMap<>();
        state.put("tenantId", execSys.getTenant());
        state.put("systemId", execSys.getId());
        state.put("jobOwner", job.getOwner());
        state.put("remoteQueue", job.getRemoteQueue());
        
        // Capture or dummy up the execution system limits.
        if (execSys.getJobMaxJobs() != null)
        	state.put("maxSystemJobs", Integer.toString(execSys.getJobMaxJobs()));
          else state.put("maxSystemJobs", DEFAULT_MAX_SYSTEM_JOBS.toString());
        if (execSys.getJobMaxJobsPerUser() != null)
            state.put("maxSystemUserJobs", Integer.toString(execSys.getJobMaxJobsPerUser()));
          else state.put("maxSystemUserJobs", DEFAULT_MAX_SYSTEM_JOBS_PER_USER.toString());

        // The batchqueue should never be null, but we check to be safe.
        // In either case we always assign a non-null value to each key.
        if (remoteQueue == null) {
            state.put("maxQueueJobs", Long.toString(DEFAULT_MAX_JOBS));
            state.put("maxUserQueueJobs", Long.toString(DEFAULT_MAX_USER_JOBS));
        } else {
            state.put("maxQueueJobs", Long.toString(remoteQueue.getMaxJobs()));
            state.put("maxUserQueueJobs", Long.toString(remoteQueue.getMaxJobsPerUser()));
        }
        
        return state;
    }

    /* ---------------------------------------------------------------------- */
    /* updateJobActivity:                                                     */
    /* ---------------------------------------------------------------------- */
    /** If it is a recoverable exception then update state to indicate the activity in progress
     *    when the exception occurred.
     *  If state is null then initialize it.
     *  If activity has already been recorded then do not change it.
     *
     * @param e Exception to update
     * @param activityStr current activity
     */
    public static void updateJobActivity(Exception e, String activityStr)
    {
        // If no exception or no activity string provided then nothing to do.
        if (e == null || StringUtils.isBlank(activityStr)) return;
        // If not a recoverable exception then nothing to do.
        if (!(e instanceof TapisRecoverableException)) return;
        // It is a recoverable exception, create state map if necessary.
        TapisRecoverableException rex = (TapisRecoverableException) e;
        if (rex.state == null) rex.state = new TreeMap<>();
        // Update the the BlockedJobActivity state if it is not already set.
        String tmpStr = rex.state.get(JobRecoveryDefinitions.BLOCKED_JOB_ACTIVITY);
        if (StringUtils.isBlank(tmpStr))
        {
            rex.state.put(JobRecoveryDefinitions.BLOCKED_JOB_ACTIVITY, activityStr);
        }
  }


    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* handleTrigger:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Wrap the exception e in a recoverable job exception based on the type
     * of the trigger exception.
     */
    private static JobRecoverableException handleTrigger(TapisException e, 
                                                         JobExecutionContext jobCtx,
                                                         TapisSSHConnectionException trigger)
    {
        // Create a recoverable exception that wraps the original.
        // For SSH connection failures, the StepwiseBackoffPolicy will be used.
        // Setup the parameters to retry about every minute for 5 attempts then
        //       every 2 minutes for 5 attempts and finally every 5 minutes
        //       every 5 minutes for 3 attempts.
        // Set the maximum time to retry to 3 hours as a fallback
        ArrayList<Pair<Integer,Long>> steps = new ArrayList<>();
        steps.add(Pair.of(5,  60000L));   // 1 minute
        steps.add(Pair.of(5, 120000L));   // 2 minutes
        steps.add(Pair.of(3, 300000L));   // 5 minutes
        Gson gson = TapisGsonUtils.getGson(true);
        String stepsStringValue = gson.toJson(steps);

        TreeMap<String, String> policyParameters = new TreeMap<>();
        policyParameters.put("steps", stepsStringValue);
        policyParameters.put("maxElapsedSeconds", "10800000"); // 3 hours
        JobRecoverMsg jobRecoverMsg = JobRecoverMsgFactory.getJobRecoverMsg(
                                        RecoveryConfiguration.DFT_CONNECTION_FAILURE, 
                                        jobCtx.getJob(), RecoveryUtils.class.getSimpleName(), 
                                        e.getMessage(), policyParameters, trigger.state);
        return new JobSSHConnectionException(jobRecoverMsg, e.getMessage(), e);
    }

  /* ---------------------------------------------------------------------- */
  /* handleTrigger:                                                         */
  /* ---------------------------------------------------------------------- */
  /** Wrap the exception e in a recoverable job exception based on the type
   * of the trigger exception.
   */
  private static JobRecoverableException handleTrigger(TapisException e,
                                                       JobExecutionContext jobCtx,
                                                       TapisSSHAuthException trigger)
  {
    // Set the default recovery configuration.
    RecoveryConfiguration config;
    TreeMap<String,String> policyParms = new TreeMap<>();

    // Knowing what command context the authentication error occurred in
    //   allows us to choose the appropriate recovery configuration.
    // If this is a first time auth failure as indicated by activity of
    //   CHECK_SYSTEMS then it may be a fundamental auth problem (e.g. bad keys)
    //   so use special config that does very limited re-tries.
    // Else it is possibly an intermittent auth failure so use the default
    //   config that allows for more re-tries.
    String activity = trigger.state.get(JobRecoveryDefinitions.BLOCKED_JOB_ACTIVITY);
    if (activity != null && activity.equals(BlockedJobActivity.CHECK_SYSTEMS.name())) {
      // Set the configuration type.
      config = RecoveryConfiguration.FIRST_AUTHENTICATION_FAILURE;

      // Set constant backoff policy parameters to stick with the default
      // wait time but reduce the number of retries.
      policyParms.put("maxTries", Integer.toString(3));  // wait 3 minutes max
    }
    else {
      // Set the default configuration type and take the policy defaults.
      config = RecoveryConfiguration.DFT_AUTHENTICATION_FAILURE;

      // Set constant backoff policy parameters to stick with the default
      // wait time but reduce the number of retries.
      policyParms.put("maxTries", Integer.toString(15));  // wait 15 minutes max
    }

    // Create a recoverable exception that includes the original information.
    JobRecoverMsg jobRecoverMsg = JobRecoverMsgFactory.getJobRecoverMsg(
            config,
            jobCtx.getJob(), RecoveryUtils.class.getSimpleName(),
            e.getMessage(), policyParms, trigger.state);
    return new JobSSHAuthException(jobRecoverMsg, e.getMessage(), e);
  }

    /* ---------------------------------------------------------------------- */
    /* handleTrigger:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Wrap the exception e in a recoverable job exception based on the type
     * of the trigger exception.
     */
    private static JobRecoverableException handleTrigger(TapisException e, 
                                                         JobExecutionContext jobCtx,
                                                         TapisSystemAvailableException trigger)
    {
        // Create a recoverable exception that wraps the original.
        JobRecoverMsg jobRecoverMsg = JobRecoverMsgFactory.getJobRecoverMsg(
                                        RecoveryConfiguration.DFT_SYSTEM_NOT_AVAILABLE, 
                                        jobCtx.getJob(), RecoveryUtils.class.getSimpleName(), 
                                        e.getMessage(), null, trigger.state);
        return new JobSystemAvailableException(jobRecoverMsg, e.getMessage(), e);
    }
    
    /* ---------------------------------------------------------------------- */
    /* handleTrigger:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Wrap the exception e in a recoverable job exception based on the type
     * of the trigger exception.
     */
    private static JobRecoverableException handleTrigger(TapisException e, 
                                                         JobExecutionContext jobCtx,
                                                         TapisAppAvailableException trigger)
    {
        // Create a recoverable exception that wraps the original.
        JobRecoverMsg jobRecoverMsg = JobRecoverMsgFactory.getJobRecoverMsg(
                                        RecoveryConfiguration.DFT_APPLICATION_NOT_AVAILABLE, 
                                        jobCtx.getJob(), RecoveryUtils.class.getSimpleName(), 
                                        e.getMessage(), null, trigger.state);
        return new JobAppAvailableException(jobRecoverMsg, e.getMessage(), e);
    }
    
    /* ---------------------------------------------------------------------- */
    /* handleTrigger:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Wrap the exception e in a recoverable job exception based on the type
     * of the trigger exception.
     */
    private static JobRecoverableException handleTrigger(TapisException e, 
                                                         JobExecutionContext jobCtx,
                                                         TapisQuotaException trigger)
    {
        // Create a recoverable exception that wraps the original.
        JobRecoverMsg jobRecoverMsg = JobRecoverMsgFactory.getJobRecoverMsg(
                                        RecoveryConfiguration.DFT_QUOTA_EXCEEDED, 
                                        jobCtx.getJob(), RecoveryUtils.class.getSimpleName(), 
                                        e.getMessage(), null, trigger.state);
        return new JobAppAvailableException(jobRecoverMsg, e.getMessage(), e);
    }
}

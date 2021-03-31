package edu.utexas.tacc.tapis.jobs.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoverableException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoveryDefinitions;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoveryDefinitions.BlockedJobActivity;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.messages.JobSubmitMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.QuotaChecker;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

final class JobQueueProcessor 
  extends AbstractProcessor
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobQueueProcessor.class);
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // The job-specific topic thread spawned when processing a job.
  private JobTopicThread _jobTopicThread;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  JobQueueProcessor(JobWorker jobWorker){super(jobWorker);}
  
  /* ********************************************************************** */
  /*                           Protected Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getNextMessage:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Provide parameter for super class message retrieval method. */  
  @Override
  protected void getNextMessage()
   throws TapisRuntimeException
  {
      // Initialize channel parameters.
      String queueName  = _jobWorker.getParms().queueName;
      
      // Read messages from the tenant topic queue that bind to:
      //
      //    tapis.jobq.<user-selected-queue-name-suffix>
      //
      String[] bindingKeys = new String[] {queueName};
      
      // All error handling is performed by super class.
      NextMessageParms p = new NextMessageParms(JobQueueManagerNames.getSubmitExchangeName(), 
                                                BuiltinExchangeType.DIRECT, 
                                                queueName, 
                                                bindingKeys);
      getNextMessage(p);
  }

  /* ---------------------------------------------------------------------- */
  /* process:                                                               */
  /* ---------------------------------------------------------------------- */
  /** Process a delivered message.
   * 
   * Handling JobAsyncCmdExceptions
   * ------------------------------
   * JobAsyncCmdException are thrown when an asynchronous pause or cancel 
   * command is received by a job.  The job discovers that it has received
   * an asynchronous command by checking its cmdMsg field at convenient 
   * intervals during processing.  All methods that perform the check have
   * the possibility of throwing the exception, which curtails all current
   * job processing.  Any method that calls a method that performs the check
   * should allow the exception to pass up the call stack as is and take
   * no further action (e.g., no logging). 
   * 
   * This top-level job processing method is the ultimate receiver of 
   * JobAsyncCmdExceptions.  When one is received, it simply terminates
   * job processing as if the job completed successfully.  It is the 
   * responsibility of the asynchronous command processor to place the 
   * job into a proper state.
   * 
   * @param delivery the incoming message and its metadata
   * @return true if the message was successfully processed, false if the 
   *          message should be rejected and discarded without redelivery
   */
  @Override
  protected boolean process(DeliveryResponse delivery)
  {
    // Tracing
    if (_log.isDebugEnabled()) { 
        String msg = JobQueueManager.getInstance().dumpMessageInfo(
          delivery.consumerTag, delivery.envelope, delivery.properties, delivery.body);
        _log.debug(msg);
    }
    
    // Execute the job.
    JobExecutionContext jobCtx = null;
    boolean ack = true; // be optimistic that things will succeed.
    JobSubmitMsg jobMsg = null;
    
    try {
      // Reconstitute the job submit message.
      String body = new String(delivery.body);
      jobMsg = TapisGsonUtils.getGson(true).fromJson(body, JobSubmitMsg.class);
      
      // Determine if new or existing job processing is required.
      JobsDao jobsDao = new JobsDao();
      Job job = jobsDao.getJobByUUID(jobMsg.getUuid());
      
      // Do we have a record of this job?
      if (job == null) {
          String msg = MsgUtils.getMsg("JOBS_UNKNOWN_JOB_QUEUED", _jobWorker.getParms().name, 
        		                       jobMsg.getUuid());
          _log.error(msg);
    	  sendUnknownJobEmail(jobMsg.getUuid(), msg);
    	  throw new JobException(msg);
      }
      
      // Create the execution context used for the remainder of job processing.
      // Threadlocal fields are set here and reference to the context is also
      // stored in the job.  
      //
      // We also get a fresh notification list from the db, which is necessary
      // on resubmissions.
      jobCtx = new JobExecutionContext(job, jobsDao);
      
      // Remove references to the job outside of the context object.
      job = null;
      
      // Start the job-specific topic thread after completing job initialization so that
      // the changes to the job on this thread happen before the topic thread starts.
      startJobTopicThread(jobCtx.getJob());

      // Begin job processing.  Swallow exceptions that indicate an
      // asynchronous command has interrupted normal processing to
      // put the job into a inactive or terminal state.  All other
      // exceptions are handled by the enclosing try block.
      try {ack = processJob(jobCtx);}
          catch (JobAsyncCmdException e) {}
    }
    catch (Exception e) {
        // Initialize the job if one exists.
        Job job = null;
        if (jobCtx != null) job = jobCtx.getJob();
        
        // Leave breadcrumbs.
        JobWorkerThread thd = (JobWorkerThread) Thread.currentThread();
        String jobUuid = job == null ? jobMsg.getUuid() : job.getUuid();
        String msg = MsgUtils.getMsg("JOBS_WORKER_PROCESSING_ERROR", thd.getName(), _queueName, 
                                     getProcessorName(), jobUuid, e.getMessage());
        _log.error(msg, e);
        
        // Leave now if we don't have a job.
        if (job == null) { setFinalMessageToNull(jobCtx); return false; }
        
        // Check for a cancel command that occurred after the exception or
        // while the worker thread was blocked on i/o and never had a chance
        // to check again.  Cancellation takes precedence over recovery.
        if (jobCtx.checkForCancelBeforeRecovery()) { setFinalMessageToNull(jobCtx); return false; }
        
        // ------------ Recoverable Job Exception
        // See if we caught a recoverable exception or one that can be turned into a recoverable exception.
        if (e instanceof TapisException) {
            // Is this a recoverable situation?
            JobRecoverableException rex = RecoveryUtils.makeJobRecoverableException((TapisException)e, jobCtx);
            
            // Requeue recoverable exceptions on retry queue and return.
            // If false is return, then the attempt to put the job into
            // recovery failed and the job itself must be abandoned.
            if (rex == null) ack = false;
              else {
            	  ack = putJobIntoRecovery(job, rex);
            	  setFinalMessageToNull(jobCtx);
              }
        } 
        else ack = false; // Causes job to fail and be abandoned
        
        // ------------ Unrecoverable Job Exception
        // If we get here with a negative ack, we have to fail the job. 
        //
        if (!ack) failJob(job, msg);
        
        // Clean up context.
        jobCtx.close();
    }
    finally {
      // We always want to check the finalMessage field. 
      if (jobCtx != null) checkFinalMessageField(jobCtx);
    	
      // Always interrupt and clear the job-specific thread 
      // spawned by this thread (if it exists) when we are
      // finished processing the current job.
      interruptJobTopicThread();
    }
    
    // TODO: need more than just ack and reject-discard; exception handling needs thought.
    return ack;
  }
  
  /* ********************************************************************** */
  /*                            Private Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* setJobTopicThread:                                                     */
  /* ---------------------------------------------------------------------- */
  /** Serialize access the job thread field whenever it's updated.
   * 
   * @param thread the job-specific thread spawned by this thread
   */
  private synchronized void setJobTopicThread(JobTopicThread thread) {_jobTopicThread = thread;}

  /* ---------------------------------------------------------------------- */
  /* startJobTopicThread:                                                   */
  /* ---------------------------------------------------------------------- */
  private void startJobTopicThread(Job job)
  {
    // Spawn the job-specific thread and save its reference in _jobTopicThread. 
    JobTopicThread jobTopicWorker = new JobTopicThread(Thread.currentThread().getName(), job);
    jobTopicWorker.setDaemon(true);
    jobTopicWorker.setUncaughtExceptionHandler(_jobWorker);
    jobTopicWorker.start();
  }
  
  /* ---------------------------------------------------------------------- */
  /* interruptJobTopicThread:                                               */
  /* ---------------------------------------------------------------------- */
  /** Serialize access to the job thread field whenever it's updated. */
  private synchronized void interruptJobTopicThread()
  {
    if (_jobTopicThread == null) return;
    _jobTopicThread.interrupt();
    _jobTopicThread = null;
  }
  
  /* ---------------------------------------------------------------------- */
  /* createJobThreadName:                                                   */
  /* ---------------------------------------------------------------------- */
  /** The job-specific thread name uses the spawning thread name as prefix. */
  private String createJobThreadName(String workerThreadName) {
    return workerThreadName + JobWorker.JOB_TOPIC_THREAD_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* checkFinalMessageField:                                                */
  /* ---------------------------------------------------------------------- */
  /** Check to see if the finalMessage on the JobCtx is null. If it 
   *  is not null, we update the lastMessage field in the database          
   *  with the finalMessage.                                                */
  private void checkFinalMessageField(JobExecutionContext jobCtx) {
	String finalMessage = jobCtx.getFinalMessage(); 
    if (finalMessage == null) return; 
    
    // If finalMessage is not null at this point, we will want to update the lastMessage
    // field in the database with this finalMessage. 
    Job job = jobCtx.getJob();
    if (job != null) {
    	_log.error(finalMessage);
    	jobCtx.getJobsDao().updateLastMessageWithFinalMessage(finalMessage, job);
    }
  }
  
  /* ---------------------------------------------------------------------- */
  /* setFinalMessageToNull:                                                */
  /* ---------------------------------------------------------------------- */
  /** In certain scenarios, we do not want to update last message with any 
   * final message. In those cases, we use this method to ensure finalMessage
   * is null. 
   * @param jobCtx
   */
  private void setFinalMessageToNull(JobExecutionContext jobCtx) {
	  if (jobCtx == null) return; 
	  jobCtx.setFinalMessage(null);
  }
 
//  /* ---------------------------------------------------------------------- */
//  /* getJobNotifications:                                                   */
//  /* ---------------------------------------------------------------------- */
//  private List<Notification> getJobNotifications(Job job) 
//   throws AloeException
//  {
//      // Retrieve all notifications attached to this job.
//      NotificationDao notifDao = new NotificationDao(JobDao.getDataSource());
//      return notifDao.getNotificationsByAssociatedUUID(job.getUuid());
//  }
  
  /* ---------------------------------------------------------------------- */
  /* processJob:                                                            */
  /* ---------------------------------------------------------------------- */
  /** Pick up processing for a job that has a record in the jobs table.
   * 
   * @param jobCtx an existing job.
   * @return true if the job was successfully processed, false if the 
   *          job should be rejected and discarded without redelivery.
   * @throws TapisException on recoverable or unrecoverable error
   * @throws JobAsyncCmdException when an asynchronous command stops or postpones execution 
   */
  private boolean processJob(JobExecutionContext jobCtx) 
   throws TapisException, JobAsyncCmdException
  {
      // Unpack job for convenience.
      Job job = jobCtx.getJob();
      
      // Begin processing message.
      if (_log.isDebugEnabled()) 
          _log.debug("\n------------------------\nProcessing job:\n" + 
                     TapisUtils.toString(job) + "\n------------------------\n");
      
      // ------------------ Initial validation --------------------
      // Validate the job specification.
      try {job.validateForExecution();}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("JOBS_INVALID_JOB", job.getUuid(), e.getMessage());
              throw JobUtils.tapisify(e, msg);
          }
    
      // Check for non-runnable state. This includes the BLOCKED state
      // which means that the job has experienced a recoverable error
      // and is undergoing recovery processing.  See putJobIntoRecovery()
      // for details.
      if (!job.getStatus().isActive()) {
          String msg = MsgUtils.getMsg("JOBS_INACTIVE_JOB_REMOVED", job.getUuid(), job.getStatus());
          _log.warn(msg);
          return false;
      }
    
      // Set the default return code to cause a positive ack to rabbitmq.
      boolean rc = true;
      
      // ------------------ Run the Job ---------------------------
      // The main processing loop advances state to state.
      boolean keepProcessing = true;
      while (keepProcessing) {
          // Use the result of each case to determine if we iterate.
          keepProcessing = switch (job.getStatus()) {
          
              // Normal processing states.
              case PENDING           -> doPending(job);
              case PROCESSING_INPUTS -> doProcessingInputs(job);
              case STAGING_INPUTS    -> doStagingInputs(job);
              case STAGING_JOB       -> doStagingJob(job);
              case SUBMITTING_JOB    -> doSubmittingJob(job);
              case QUEUED            -> doQueued(job);
              case RUNNING           -> doRunning(job);
              case ARCHIVING         -> doArchiving(job);

              // Terminal states.
              case CANCELLED         -> {rc = false; yield false;}
              case FAILED            -> {rc = false; yield false;}
              case FINISHED          -> false;
              
              // States that should never be encountered here.
              case BLOCKED           -> throw new JobException(MsgUtils.getMsg(
                                            "JOBS_UNEXPECTED_STATUS", job.getUuid(), JobStatusType.BLOCKED));
              case PAUSED            -> throw new JobException(MsgUtils.getMsg(
                                            "JOBS_UNEXPECTED_STATUS", job.getUuid(), JobStatusType.PAUSED));
              
              // Unaccounted for state!
              default                -> throw new JobException(MsgUtils.getMsg(
                                            "JOBS_UNKNOWN_STATUS", job.getUuid(), job.getStatus()));
          };
      }
      
      // Acknowledge the queue message.
      return rc;
      
//      // ================== Pre-Phase Processing ==================
//      // ------------------ Initial validation --------------------
//      // Validate the job specification.
//      try {job.validateForExecution();}
//          catch (Exception e) {
//              String msg = MsgUtils.getMsg("JOBS_INVALID_JOB", job.getUuid(), e.getMessage());
//              _log.error(msg, e);
//              throw JobUtils.aloeify(e, msg);
//          }
//      
//      // Check for non-runnable state. This includes the BLOCKED state
//      // which means that the job has experienced a recoverable error
//      // and is undergoing recovery processing.  See putJobIntoRecovery()
//      // for details.
//      if (!job.getStatus().isActive()) {
//          String msg = MsgUtils.getMsg("JOBS_INACTIVE_JOB_REMOVED", job.getUuid(), job.getStatus());
//          _log.warn(msg);
//          return false;
//      }
//      
//      // ------------------ Initialize Exec System ---------------- 
//      // Let the context cache the execution system now and keep a local
//      // reference to avoid unnecessary exception handling below.
//      try {jobCtx.getExecutionSystem();}
//          catch (Exception e) {
//              String msg = MsgUtils.getMsg("JOBS_EXEC_SYSTEM_RETRIEVAL_FOR_JOB_ERROR",
//                                           job.getSystemId(), job.getTenantId(), job.getUuid(),
//                                           e.getMessage());
//              _log.error(msg, e);
//              throw JobUtils.aloeify(e, msg);
//          }
//      
//      // *** Async command check ***
//      jobCtx.checkCmdMsg();
//      
//      // ------------------ Systems Check -------------------------
//      // Creates the output directory on the archive system and 
//      // authenticates with the execution system to make sure the
//      // user can logon.  No need to do extra logging here.
//      try {checkExecAndArchiveSystems(jobCtx);}
//          catch (Exception e) {throw JobUtils.aloeify(e);}
//      
//      // ------------------ Quota Checks --------------------------
//      // Determine if the job can proceed based on the various aloe
//      // quotas placed on the execution systems, users and batchqueues.
//      try {(new QuotaChecker(jobCtx)).checkQuotas();;}
//          catch (Exception e) {throw JobUtils.aloeify(e);}
//      
//      // ================== Phase Processing ======================
//      // ------------------ Stage the job inputs ------------------
//      if (AbstractPhase.jobInPhase(job, JobPhase.STAGING)) {
//          StagingPhase stagingPhase = new StagingPhase(jobCtx);
//          try {stagingPhase.execute();}
//              catch (JobAsyncCmdException e) {throw e;}
//              catch (Exception e) {
//                  String msg = MsgUtils.getMsg("JOBS_STAGING_PHASE_ERROR", job.getUuid(), 
//                                               job.getOwner(), job.getTenantId(), job.getAppId(),
//                                               e.getMessage());
//                  _log.error(msg, e);
//                  throw JobUtils.aloeify(e, msg);
//              }
//          stagingPhase = null; // allow memory reclamation
//      }
//      
//      // ------------------ Submit for execution ------------------
//      if (AbstractPhase.jobInPhase(job, JobPhase.SUBMITTING)) {
//          SubmittingPhase submittingPhase = new SubmittingPhase(jobCtx);
//          try {submittingPhase.execute();}
//              catch (JobAsyncCmdException e) {throw e;}
//              catch (Exception e) {
//                  String msg = MsgUtils.getMsg("JOBS_SUBMITTING_PHASE_ERROR", job.getUuid(), 
//                                               job.getOwner(), job.getTenantId(), job.getAppId(),
//                                               e.getMessage());
//                  _log.error(msg, e);
//                  throw JobUtils.aloeify(e, msg);
//              }
//          submittingPhase = null; // allow memory reclamation
//      }
//      
//      // ------------------ Monitor execution ---------------------
//      if (AbstractPhase.jobInPhase(job, JobPhase.MONITORING)) {
//          MonitoringPhase monitoringPhase = new MonitoringPhase(jobCtx);
//          try {monitoringPhase.execute();}
//              catch (JobAsyncCmdException e) {throw e;}
//              catch (Exception e) {
//                  String msg = MsgUtils.getMsg("JOBS_MONITORING_PHASE_ERROR", job.getUuid(), 
//                                               job.getOwner(), job.getTenantId(), job.getAppId(),
//                                               e.getMessage());
//                  _log.error(msg, e);
//                  throw JobUtils.aloeify(e, msg);
//              }
//          monitoringPhase = null; // allow memory reclamation
//      }
//      
//      // ------------------ Archive job ---------------------------
//      if (AbstractPhase.jobInPhase(job, JobPhase.ARCHIVING)) {
//          ArchivingPhase archivingPhase = new ArchivingPhase(jobCtx);
//          try {archivingPhase.execute();}
//              catch (JobAsyncCmdException e) {throw e;}
//              catch (Exception e) {
//                  String msg = MsgUtils.getMsg("JOBS_ARCHIVING_PHASE_ERROR", job.getUuid(), 
//                                               job.getOwner(), job.getTenantId(), job.getAppId(),
//                                               e.getMessage());
//                  _log.error(msg, e);
//                  throw JobUtils.aloeify(e, msg);
//              }
//          archivingPhase = null; // allow memory reclamation
//      }
  }
  
  /* ---------------------------------------------------------------------- */
  /* doPending:                                                             */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doPending(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
    
      // ------------------ Quota Checks --------------------------
      // Determine if the job can proceed based on the various tapis
      // quotas placed on the execution systems, users and batchqueues.
      try {(new QuotaChecker(jobCtx)).checkQuotas();}
          catch (Exception e) {handleException(job, e, BlockedJobActivity.CHECK_QUOTA);}
      
      // Advance job to next state.
      setState(job, JobStatusType.PROCESSING_INPUTS);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doProcessingInputs:                                                    */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doProcessingInputs(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
      
      // Create the input and output directories useb by this job.
      try {jobCtx.createDirectories();}
          catch (Exception e) {handleException(job, e, BlockedJobActivity.PROCESSING_INPUTS);}
    
      // Advance job to next state.
      setState(job, JobStatusType.STAGING_INPUTS);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doStagingInputs:                                                       */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doStagingInputs(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
      
      // Stage inputs.
      try {jobCtx.stageInputs();}
      catch (Exception e) {handleException(job, e, BlockedJobActivity.STAGING_INPUTS);}

      // Advance job to next state.
      setState(job, JobStatusType.STAGING_JOB);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doStagingJob:                                                          */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doStagingJob(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
    
      // Stage job.
      try {jobCtx.stageJob();}
      catch (Exception e) {handleException(job, e, BlockedJobActivity.STAGING_JOB);}

      // Advance job to next state.
      setState(job, JobStatusType.SUBMITTING_JOB);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doSubmittingJob:                                                       */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doSubmittingJob(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
    
      // Submit job.
      try {jobCtx.submitJob();}
      catch (Exception e) {handleException(job, e, BlockedJobActivity.SUBMITTING);}

      // Advance job to next state.
      setState(job, JobStatusType.QUEUED);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doQueued:                                                              */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doQueued(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
    
      // Check queued job.
      try {jobCtx.monitorQueuedJob();}
      catch (Exception e) {handleException(job, e, BlockedJobActivity.QUEUED);}

      // Advance job to next state. 
      setState(job, JobStatusType.RUNNING);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doRunning:                                                             */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doRunning(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
    
      // Check the remote running job unless it has already reached a terminal
      // state, in which case there's not need for further monitoring.
      if (job.getRemoteOutcome() == null)
          try {jobCtx.monitorRunningJob();}
          catch (Exception e) {handleException(job, e, BlockedJobActivity.RUNNING);}

      // Advance job to next state.
      setState(job, JobStatusType.ARCHIVING);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doArchiving:                                                           */
  /* ---------------------------------------------------------------------- */
  /** This method processes jobs that are in its named state.  This processing 
   * always results in one of the following outcomes:
   * 
   *  - return true to continue job processing in a new state
   *  - throw a recoverable exception which will cause the job to be put into
   *      recovery in a blocked state
   *  - throw async exception as the result of receiving certain asynchronous commands
   *  - throw an unrecoverable exception to fail this job
   * 
   * @param job the currently executing job
   * @return true to continue processing the job, false to quit processing 
   * @throws TapisException on recoverable or unrecoverable condition
   * @throws JobAsyncCmdException on terminating asynchronous command 
   */
  private boolean doArchiving(Job job)
   throws TapisException, JobAsyncCmdException
  {
      // *** Async command check ***
      var jobCtx = job.getJobCtx(); 
      jobCtx.checkCmdMsg();
    
      // Stage inputs.
      try {jobCtx.archiveOutputs();}
      catch (Exception e) {handleException(job, e, BlockedJobActivity.ARCHIVING);}

      // Advance job to next state.
      setState(job, JobStatusType.FINISHED);
      
      // True means continue processing the job.
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* handleException:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Change the job status to FAILED on unrecoverable errors, otherwise 
   * leave the state alone so that the recovery code can put the job into the
   * BLOCKED state capturing the appropriate details.
   * 
   * Except on database update errors, this method always throws the original 
   * exception as-is or wrapped.
   * 
   * @param job the job that experienced an exception
   * @param e the exception
   * @throws TapisException a new db error or the possibly wrapped original exception
   */
  private void handleException(Job job, Exception e, BlockedJobActivity activity) 
   throws TapisException
  {
      // Assign blocked activity in recoverable exceptions.
      RecoveryUtils.updateJobActivity(e, activity.name());
      
      // See if the exception or any of it ancestors in the causal 
      // chain are recoverable.  If not, the exception indicates the
      // job has failed, so we set the state accordingly.
      final var exArray = new Class<?>[] {TapisRecoverableException.class,
                                          JobRecoverableException.class};
      if (TapisUtils.findInChain(e, exArray) == null) 
          setState(job, JobStatusType.FAILED);
          
      // Always throw the massaged exception from here. 
      throw JobUtils.tapisify(e);
  }
  
  /* ---------------------------------------------------------------------- */
  /* setState:                                                              */
  /* ---------------------------------------------------------------------- */
  private void setState(Job job, JobStatusType newStatus) throws JobException
  {setState(job, newStatus, null);}
  
  /* ---------------------------------------------------------------------- */
  /* setState:                                                              */
  /* ---------------------------------------------------------------------- */
  /** Update the job's database record and in memory object to reflect the
   * transition to the new status.  If the update message is null the default
   * state change message will be saved in the database.  
   * 
   * Currently, there is no recovery from a failure to update status.
   * 
   * @param job the executing job 
   * @param newStatus a legal new status for the job
   * @param msg the update message or null
   * @throws JobException 
   */
  private void setState(Job job, JobStatusType newStatus, String msg) throws JobException
  {
      // Get the context.
      var jobCtx = job.getJobCtx();
      jobCtx.getJobsDao().setStatus(job, newStatus, msg);
  }
  
  /* ---------------------------------------------------------------------- */
  /* putJobIntoRecovery:                                                    */
  /* ---------------------------------------------------------------------- */
  /** A recoverable condition was detected during job processing.  We put the
   * job into a BLOCKED state and post a recovery message on the tenant's
   * recovery queue.  The recovery reader process manages the job while it's
   * in recovery.
   * 
   * Zombie Warning
   * --------------
   * The recovery protocol implemented in this method has the following 
   * characteristics:
   * 
   *   a) The job's status is first changed to BLOCKED.
   *   b) An attempt to post a recovery message is only made if the status 
   *      change was committed. 
   *      
   * If either step fails an attempt is made to fail the job. A catastrophic
   * failure between steps a) and b) will lead to a zombie job (one that is
   * BLOCKED forever since the job never makes it to the recovery subsystem).
   * This can be prevented using 2-phase commit and a transaction manager, 
   * otherwise jobs can become zombies.
   * 
   * In general, jobs will be left in inconsistent states if their statuses
   * cannot be updated.  If step a) does not succeed and the subsequent 
   * attempt to fail the job also fails, the job will be in a non-terminal
   * state but still removed from its submission queue.  If step a) does
   * succeed, but the job cannot be posted to the recovery queue, and then
   * failing the job does not succeed, the job is again left in an 
   * inconsistent state.  These are both ways a job can become a zombie.
   * 
   * A more elaborate recovery mechanism would take into account the fact
   * the the database, the messaging system or both could fail at any time.
   * In the future, we might choose to close some or all of these failure 
   * windows, or we might implement zombie detection programs that clean up
   * jobs left in inconsistent states.   
   *      
   * @param job the blocked job
   * @param recoverableException the exception with wait condition information
   * @return true if the job was place into recovery, false otherwise
   */
  private boolean putJobIntoRecovery(Job job, JobRecoverableException recoverableException)
  {
      // Make sure the recoverable execution contains a recovery message.
      if ((recoverableException == null) || (recoverableException.jobRecoverMsg == null)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "putJobIntoRecovery", "recoverableException");
          _log.error(msg);
          
          // Fail the job.
          String failMsg =  MsgUtils.getMsg("JOBS_STATUS_FAILED_IMPROPER_RECOVERY");
          failJob(job, failMsg);
          _log.error(failMsg);
          
          // There's nothing more we can do.
          return false;
      }
      
      // Set up.
      boolean ack = true;    // Assume no problems.
      String failMsg = null; // Failure message included in job record.
      
      // Change the job status.
      try
      {
          // Put the job into a BLOCKED state. Things can go badly if this fails.
          JobsDao dao = new JobsDao();
          dao.setStatus(job, JobStatusType.BLOCKED, recoverableException.jobRecoverMsg.getStatusMessage());
      } 
      catch (Exception e) {
          // Declare a failure.
          ack = false;
          
          // Log the exception but don't rethrow it.
          failMsg = MsgUtils.getMsg("JOBS_STATUS_CHANGE_ERROR", job.getUuid(), JobStatusType.BLOCKED);
          _log.error(failMsg, e);
      }
      
      /* ----------------------------------------------------
       * Catastrophic failure here leads to an inconsistency:
       * 
       *  - A BLOCKED job is never delivered to the recovery
       *    subsystem; it will stay blocked forever.
       * ----------------------------------------------------
       */
      
      // On success the job is blocked, so we post the recovery message to the tenant  
      // recovery queue. Failure here leads to an inconsistent state where the job is 
      // blocked but it cannot be sent to the recovery manager. We will try to fail the 
      // job below if this happens.
      if (ack) 
          try {
              JobQueueManager qm = JobQueueManager.getInstance();
              qm.postRecoveryQueue(recoverableException.jobRecoverMsg);
          }
          catch (Exception e) {
              // Declare a failure.
              ack = false;
          
              // Log the exception but don't rethrow it.
              failMsg = MsgUtils.getMsg("JOBS_QUEUE_POST_RECOVERY_QUEUE", job.getUuid(), 
                                        job.getTenant(), job.getOwner(), e.getMessage());
              _log.error(failMsg, e);
          }
      
      // Last ditch effort to avoid inconsistencies by failing the job when there's
      // been an error. If the attempt above to change the status failed, we may not 
      // be able to fail the job since that also involves a status change.
      if (!ack) {
          // Fail the job.
          String msg = MsgUtils.getMsg("JOBS_PUT_IN_RECOVERY_ERROR", job.getUuid(), 
                                       job.getTenant(), job.getOwner(), failMsg);
          _log.error(msg);
          failJob(job, msg);
      }
      
      return ack;
  }
  
  /* ---------------------------------------------------------------------- */
  /* failJob:                                                               */
  /* ---------------------------------------------------------------------- */
  /** Fail the job, quietly noting double faults.
   * 
   * @param job the job to fail.
   */
  private void failJob(Job job, String failMsg)
  {
      // Don't blow up.
      if (job == null) return;
      
      // Fail the job.
      try {
          JobsDao dao = new JobsDao();
          dao.failJob(_jobWorker.getParms().name, job, failMsg);
      }
      catch (Exception e) {
          // Double fault, what a mess.  The job will be left in 
          // a non-terminal state and not on any queue.  It's a zombie.
          String msg2 = MsgUtils.getMsg("JOBS_WORKER_ZOMBIE_ERROR", 
                                        _jobWorker.getParms().name,
                                        job.getUuid(), job.getTenant());
          _log.error(msg2, e);
              
          try {
              RuntimeParameters runtime = RuntimeParameters.getInstance();
              EmailClient client = EmailClientFactory.getClient(runtime);
              client.send(runtime.getSupportName(),
                          runtime.getSupportEmail(),
                          "Zombie Job Alert " + job.getUuid() + " is in a zombie state.",
                          msg2, HTMLizer.htmlize(msg2));
          }
          catch (TapisException e2) {
              // log msg that we tried to send email notice to CICSupport
              _log.error(msg2+" Failed to send support Email alert. Email client failed with exception.", e2);
          }
      }
  }
  
//  /* ---------------------------------------------------------------------- */
//  /* checkExecAndArchiveSystems:                                            */
//  /* ---------------------------------------------------------------------- */
//  /** Create the output directory on the archive system and check authentication
//   * to the execution system.
//   *  
//   * @param jobCtx the current job context
//   * @throws AloeException
//   */
//  private void checkExecAndArchiveSystems(JobExecutionContext jobCtx) 
//    throws AloeException
//  {
//      // Get information from the context.
//      Job job = jobCtx.getJob();
//      ExecutionSystem execSystem = jobCtx.getExecutionSystem();
//      
//      // If archiving is set then an archive system is guaranteed to be 
//      // non-null by the validateForExecution method.
//      RemoteSystem outputSystem;
//      try {
//          // Explicit or implicit archiving.
//          if (job.isArchive()) outputSystem = jobCtx.getArchiveSystem();
//            else outputSystem = execSystem;
//      } catch (Exception e) {
//          String msg = MsgUtils.getMsg("JOBS_ARCHIVE_SYSTEM_ASSIGNMENT", job.getUuid(), 
//                                       job.getTenantId(), e.getMessage());
//          _log.error(msg, e);
//          throw JobUtils.aloeify(e, msg);
//      }
//      
//      // Create the archive path when explicitly archiving.
//      try {jobCtx.getJobRemoteAccess().createArchivePath(outputSystem);}
//          catch (Exception e) {
//              String msg = MsgUtils.getMsg("JOBS_ARCHIVE_PATH_ERROR", job.getUuid(), 
//                                           job.getArchivePath(), outputSystem, 
//                                           e.getMessage());
//              _log.error(msg, e);
//              throw JobUtils.aloeify(e, msg);
//          }
//      
//      // Before staging any input, let make sure the execution system is accessible.
//      // If explicit archiving is on, then the execution system may not have been 
//      // accessed during archive path creation, so we do it here.
//      if (job.isArchive()) {
//          RemoteDataClient client = null;
//          try {client = jobCtx.getJobRemoteAccess().login(execSystem);}
//              catch (Exception e) {
//                  // This could be a recoverable exception so add job activity conditionally
//                  RecoveryUtils.updateJobActivity(e, JobRecoveryDefinitions.BlockedJobActivity.CHECK_SYSTEMS.name());
//                  String msg = MsgUtils.getMsg("REMOTE_LOGIN_ERROR", execSystem.getName(), 
//                                               job.getOwner(), e.getMessage());
//                  _log.error(msg, e);
//                  throw JobUtils.aloeify(e, msg);
//              }
//              finally {
//                  // Always disconnect.
//                  if (client != null) try {client.disconnect();} catch (Exception e) {}
//              }
//      }
//  }
  
  /* ---------------------------------------------------------------------- */
  /* sendUnknownJobEmail:                                                   */
  /* ---------------------------------------------------------------------- */
  /** Send an email to the support address when a job not in the jobs table
   * was queued.
   * 
   * @param jobId the unknown job uuid
   */
  private void sendUnknownJobEmail(String jobUuid, String msg)
  {
      try {
          RuntimeParameters runtime = RuntimeParameters.getInstance();
          EmailClient client = EmailClientFactory.getClient(runtime);
          client.send(runtime.getSupportName(),
                      runtime.getSupportEmail(),
                      "Unknown Job Alert: Job " + jobUuid + " is not known.",
                      msg, HTMLizer.htmlize(msg));
        }
        catch (Exception ae) {
          // log msg that we tried to send email notice to CICSupport
          _log.error(msg + " Failed to send support Email alert. Email client failed with exception.", ae);
        }
  }
  
  /* ********************************************************************** */
  /*                           JobTopicThread Class                         */
  /* ********************************************************************** */
  /** This class reads the event topic for job-specific events. */
  final class JobTopicThread extends JobWorkerThread
  {
    // Fields
    private final Job _job;
    
    // Constructor
    JobTopicThread(String workerThreadName, Job job) 
    {
      // Initialize superclass.
      super(_jobWorker.getJobThreadGroup(), createJobThreadName(workerThreadName), _jobWorker,  
            JobQueueManagerNames.getCmdSpecificJobTopicName(job.getUuid()), 
            new JobTopicProcessor(_jobWorker, job));
      
      // Save the job uuid in this thread class for restart purposes.
      _job = job;
      
      // Save a reference to this thread in enclosing class.
      // Making the assignment here allows this job-specific
      // thread's reference to be maintained in the enclosing 
      // instance even if this thread dies unexpectedly.
      setJobTopicThread(this);
    }
    
    // Allow the JobWorker to access the enclosing instance 
    // in case this thread needs to be restarted.  See 
    // JobWorker.uncaughtException() for details.
    JobQueueProcessor getEnclosing(){return JobQueueProcessor.this;}
    
    // Allow the JobWorker to access the job uuid for restart purposes.
    Job getJob(){return _job;}
  }
}

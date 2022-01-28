package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class QuotaTester 
extends AbsTester
{
   /* ********************************************************************** */
   /*                               Constants                                */
   /* ********************************************************************** */
   // Tracing.
   private static final Logger _log = LoggerFactory.getLogger(QuotaTester.class);
   
   /* ********************************************************************** */
   /*                                 Fields                                 */
   /* ********************************************************************** */
   // Set to true the first time the tester parameters are validated.
   private boolean _parmsValidated = false;
   
   // Access the jobs table.
   private JobsDao _jobsDao;
   
   // Validated test parameters saved as fields for convenient access.
   private String _tenantId;
   private String _systemId;
   private String _jobOwner;
   private String _execSystemLogicalQueue;
   private int    _maxSystemJobs;
   private int    _maxSystemUserJobs;
   private long   _maxQueueJobs;
   private long   _maxUserQueueJobs;
   
   /* **************************************************************************** */
   /*                                 Constructors                                 */
   /* **************************************************************************** */
   /* ---------------------------------------------------------------------------- */
   /* constructor:                                                                 */
   /* ---------------------------------------------------------------------------- */
   public QuotaTester(JobRecovery jobRecovery)
   {
       super(jobRecovery);
   }
   
   /* ********************************************************************** */
   /*                             Public Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* canUnblock:                                                            */
   /* ---------------------------------------------------------------------- */
   /** Return the number of jobs that should be unblocked.  Zero means don't
    * unblock any jobs.
    * 
    * @param testerParameters the quota state parameters
    */
   @Override
   public int canUnblock(Map<String, String> testerParameters) 
    throws JobRecoveryAbortException 
   {
       // Validate the tester parameters. 
       // An exception can be thrown here.
       validateTesterParameters(testerParameters);
       
       // Collect the results.
       int unblock = DEFAULT_RESUBMIT_BATCHSIZE;
       try {
           // Enforce all quotas, exiting as soon as we 
           // hit a quota violation.
           int cnt = blockedByMaxSystemJobs();
           if (cnt <= 0) return NO_RESUBMIT_BATCHSIZE;
             else unblock = Math.min(unblock, cnt);
           
           cnt = blockedByMaxSystemUserJobs();
           if (cnt <= 0) return NO_RESUBMIT_BATCHSIZE;
             else unblock = Math.min(unblock, cnt);
           
           // Only on scheduler batch jobs.
           if (!StringUtils.isBlank(_execSystemLogicalQueue)) 
           {
               cnt = blockedByMaxSystemQueueJobs();
               if (cnt <= 0) return NO_RESUBMIT_BATCHSIZE;
                 else unblock = Math.min(unblock, cnt);
           
               cnt = blockedByMaxSystemUserQueueJobs();
               if (cnt <= 0) return NO_RESUBMIT_BATCHSIZE;
                 else unblock = Math.min(unblock, cnt);
           }
       } 
       catch (Exception e) {
           String msg = MsgUtils.getMsg("JOBS_RECOVERY_QUOTA_TEST_ERROR", 
                                        _jobRecovery.getId(), _tenantId, _systemId,
                                        _jobOwner, _execSystemLogicalQueue, e.getMessage());
           _log.error(msg, e);
           throw new JobRecoveryAbortException(msg, e);
       }
       
       return unblock;
   }
   
   /* ********************************************************************** */
   /*                            Private Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* blockedByMaxSystemJobs:                                                */
   /* ---------------------------------------------------------------------- */
   /** Query the number of jobs running on the execution system.
    * 
    * @return the difference between the quota and the current count.  
    * @throws JobException on database errors
    */
   private int blockedByMaxSystemJobs() throws TapisException
   {
       // Enforce the quota.
       int jobCount = getJobsDao().countActiveSystemJobs(_tenantId, _systemId);
       return _maxSystemJobs - jobCount;
   }
   
   /* ---------------------------------------------------------------------- */
   /* blockedByMaxSystemUserJobs:                                            */
   /* ---------------------------------------------------------------------- */
   /** Query the number of jobs running on the execution system for the user.
    * 
    * @return the difference between the quota and the current count. 
    * @throws JobException on database errors
    */
   private int blockedByMaxSystemUserJobs() throws TapisException
   {
       // Enforce the quota.
       int jobCount = getJobsDao().countActiveSystemUserJobs(_tenantId, _systemId,
                                                             _jobOwner);
       return _maxSystemUserJobs - jobCount;
   }
   
   /* ---------------------------------------------------------------------- */
   /* blockedByMaxSystemQueueJobs:                                           */
   /* ---------------------------------------------------------------------- */
   /** Query the number of jobs running on the execution system and assigned
    * to the specified queue.
    * 
    * @return the difference between the quota and the current count. 
    * @throws JobException on database errors
    */
   private int blockedByMaxSystemQueueJobs() throws TapisException
   {
       // Enforce the quota.
       int jobCount = getJobsDao().countActiveSystemQueueJobs(_tenantId, _systemId,
                                                              _execSystemLogicalQueue);
       return (int) (_maxQueueJobs - jobCount);
   }
   
   /* ---------------------------------------------------------------------- */
   /* blockedByMaxSystemUserQueueJobs:                                       */
   /* ---------------------------------------------------------------------- */
   /** Query the number of jobs running on the execution system for the user
    * and assigned to the specified queue.
    * 
    * @return the difference between the quota and the current count. 
    * @throws JobException on database errors
    */
   private int blockedByMaxSystemUserQueueJobs() throws TapisException
   {
       // Enforce the quota.
       int jobCount = getJobsDao().countActiveSystemUserQueueJobs(_tenantId, _systemId,
                                                                  _jobOwner, _execSystemLogicalQueue);
       return (int) (_maxUserQueueJobs - jobCount);
   }
   
   /* ---------------------------------------------------------------------- */
   /* validateTesterParameters:                                              */
   /* ---------------------------------------------------------------------- */
   /** Validate only the parameters required to run the test. If validation 
    * fails on the first attempt, then the exception thrown will cause the
    * recovery abort and all its jobs to be failed. If validation success
    * on the first attempt, then we skip validation in this object from now on.
    * 
    * @param testerParameters the test parameter map
    * @throws JobRecoveryAbortException on validation error
    */
   private void validateTesterParameters(Map<String, String> testerParameters)
    throws JobRecoveryAbortException
   {
       // No need to validate more than once.
       if (_parmsValidated) return;

       // ------- Validate and Assign String Fields
       _tenantId = testerParameters.get("tenantId");
       if (StringUtils.isBlank(_tenantId)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "tenantId");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }

       _systemId = testerParameters.get("systemId");
       if (StringUtils.isBlank(_systemId)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "systemId");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }

       _jobOwner = testerParameters.get("jobOwner");
       if (StringUtils.isBlank(_jobOwner)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "jobOwner");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }
       
       // Can be null.
       _execSystemLogicalQueue = testerParameters.get("execSystemLogicalQueue");

       // ------- Validate and Assign Numeric Fields
       try {_maxSystemJobs = Integer.valueOf(testerParameters.get("maxSystemJobs"));}
           catch (Exception e) {
               String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateTesterParameters", 
                                            "maxSystemJobs", testerParameters.get("maxSystemJobs"));
               _log.error(msg, e);
               throw new JobRecoveryAbortException(msg, e);
           }
       
       try {_maxSystemUserJobs = Integer.valueOf(testerParameters.get("maxSystemUserJobs"));}
           catch (Exception e) {
               String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateTesterParameters", 
                                            "maxSystemUserJobs", testerParameters.get("maxSystemUserJobs"));
               _log.error(msg, e);
               throw new JobRecoveryAbortException(msg, e);
           }

       try {_maxQueueJobs = Long.valueOf(testerParameters.get("maxQueueJobs"));}
           catch (Exception e) {
               String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateTesterParameters", 
                                            "maxQueueJobs", testerParameters.get("maxQueueJobs"));
               _log.error(msg, e);
               throw new JobRecoveryAbortException(msg, e);
           }
       
       try {_maxUserQueueJobs = Long.valueOf(testerParameters.get("maxUserQueueJobs"));}
           catch (Exception e) {
               String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateTesterParameters", 
                                            "maxUserQueueJobs", testerParameters.get("maxUserQueueJobs"));
               _log.error(msg, e);
               throw new JobRecoveryAbortException(msg, e);
           }
       
       // We're good if we get here.
       _parmsValidated = true;
   }

   /* ---------------------------------------------------------------------- */
   /* getJobsDao:                                                            */
   /* ---------------------------------------------------------------------- */
   private JobsDao getJobsDao() throws TapisException
   {
       if (_jobsDao == null) _jobsDao = new JobsDao();
       return _jobsDao;
   }
}

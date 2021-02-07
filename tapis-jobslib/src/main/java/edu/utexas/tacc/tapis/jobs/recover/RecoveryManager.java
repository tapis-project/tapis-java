package edu.utexas.tacc.tapis.jobs.recover;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobRecoveryDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryExpiredException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobBlocked;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery.NextAttemptComparator;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobCancelRecoverMsg;
import edu.utexas.tacc.tapis.jobs.reader.RecoveryReader;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This singleton class is the ultimate processor of job all recovery messages.  It 
 * executes the job recovery policies specified in each JobRecovery object using the 
 * tester assigned in those objects.  It also handles job recovery cancellation
 * requests. 
 * 
 * The public methods of this class are synchronized, ensuring that only a single thread 
 * of execution will be running this class's code at a time.  Since this class is a 
 * singleton, only one recovery test at a time will run no matter how many threads
 * the calling class uses.  Though not expected, if recovery tests take a long time 
 * and JobRecovery messages regularly backup, an implementation of this class allowing 
 * greater concurrency might become necessary.  For now, the simple thread-safe 
 * approach using synchronized methods should suffice. 
 * 
 * @author rcardone
 */
public final class RecoveryManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoveryManager.class);
    
    // Wake up interval when there are no recovery records.
    private static final long DEFAULT_SLEEP_MILLIS = 3600000;  // 1 hour
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Singleton instance.
    private static RecoveryManager _instance;
    
    // Reference to the reader that ultimately spawned this instance.
    private final RecoveryReader _recoveryReader;
    
    // The set of recovery jobs in next attempt order 
    // (from soonest to latest next attempt time).
    private final TreeSet<JobRecovery> _recoveryJobs;
    
    // Index into the _recoveryJob set for lookup by testerHash.
    // Key = testerHash, value = job recovery object.
    private final HashMap<String,JobRecovery> _testerHashIndex;
    
    // Index into the _recoveryJob set for lookup by job uuid.
    // Key = job uuid, value = job recovery object.
    private final HashMap<String,JobRecovery> _jobUuidIndex;
    
    // Reuse the dao's.
    private final JobsDao        _jobsDao;
    private final JobRecoveryDao _recoveryDao;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private RecoveryManager(RecoveryReader rdr) 
     throws TapisException
    {
        // Save the reference to the reader that created us.
        _recoveryReader = rdr;
        
        // Create the ordered set of recovery jobs. 
        _recoveryJobs = new TreeSet<>(new NextAttemptComparator());
        
        // Create the indices into the recovery jobs set.
        _testerHashIndex = new HashMap<>();
        _jobUuidIndex    = new HashMap<>();
        
        // Get the dao object.
        _jobsDao = new JobsDao();
        _recoveryDao = new JobRecoveryDao();
        
        // Initialize this object from the database.
        initialize();
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Get the singleton instance of this class, creating it when necessary
     * in a race condition free manner without requiring this method to be
     * synchronized.
     * 
     * @param rdr the recovery reader that drives recovery processing.
     * @return the singleton instance
     * @throws TapisException 
     */
    public static RecoveryManager getInstance(RecoveryReader rdr) 
     throws TapisException
    {
        // Create the singleton instance upon first invocation.
        if (_instance == null) {
            synchronized (RecoveryManager.class) {
                // Test again to avoid race condition.
                if (_instance == null) _instance = new RecoveryManager(rdr);
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getMillisToWakeUp:                                                     */
    /* ---------------------------------------------------------------------- */
    /** This method returns the number of milliseconds until the earliest next
     * attempt should be tried for any recovery record.
     * 
     * When this method runs, it must be the only thread accessing the recovery
     * data structures.
     * 
     * @return the milliseconds to wait before the next recovery test should be performed
     */
    public synchronized long getMillisToWakeUp()
    {
        // Get the earliest wake up time.
        JobRecovery first = null;
        if (!_recoveryJobs.isEmpty()) first = _recoveryJobs.first();
        
        // Calculate wake up time.
        long millisToWakeUp;
        if (first == null) millisToWakeUp = DEFAULT_SLEEP_MILLIS;
         else {
            // Get the future wake up time in millis given the current time.
            long nowMillis = Instant.now().toEpochMilli();
            long firstMillis = first.getNextAttempt().toEpochMilli();
            millisToWakeUp = Math.max(0, firstMillis - nowMillis);
         }
        
        // Return the number of milliseconds to sleep.
        return millisToWakeUp;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recover:                                                               */
    /* ---------------------------------------------------------------------- */
    /** The manager tries to recover all jobs whose recovery records have a 
     * next attempt time that has already past.  If the recoveryMsg parameter is
     * not null, then the message is used to insert a new job recovery record into
     * the recovery set before processing ready records.
     * 
     * When this method runs, it must be the only thread accessing the recovery
     * data structures.
     * 
     * @param recoverMsg a new recovery record or null if this is just a wake up call.
     */
    public synchronized void recover(JobRecovery newJobRecovery)
    {
        // Add the new job recovery object to set of
        // recoverable jobs if one was passed in.
        addRecoveryJob(newJobRecovery);
        
        // Get the recovery job with the earliest next attempt time,
        // removing the record from the set.
        JobRecovery curJobRecovery = _recoveryJobs.pollFirst();
        if (curJobRecovery == null) return;  // quick return
        
        // Execute all tests whose next attempt time has arrived.
        while (curJobRecovery != null &&
               curJobRecovery.getNextAttempt().isBefore(Instant.now()))
        {
            // Recover one or more jobs if the failure condition cleared.
            doRecover(curJobRecovery);
            
            // Re-insert the current recovery job into the set if it 
            // still contains blocked jobs. This occurs when the some
            // of the jobs were resubmitted, but not all.
            if (!curJobRecovery.getBlockedJobs().isEmpty())
                _recoveryJobs.add(curJobRecovery);
            
            // Set up for the next iteration.
            curJobRecovery = _recoveryJobs.pollFirst();
        }
        
        // Put the last recovery job back in the set since
        // we got here because its attempt time has not arrived.
        if (curJobRecovery != null) _recoveryJobs.add(curJobRecovery);
    }
    
    /* ---------------------------------------------------------------------- */
    /* cancelRecovery:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Remove the job specified in the input message from recovery. If the 
     * job's recovery is cancelled, return true, otherwise return false.
     * 
     * This method runs on a short-lived thread and runs concurrent to the
     * main recovery thread.  Since cancellation requires reading and 
     * writing the recovery data structures, this method run a in mutually
     * exclusive way to all other recovery threads.
     * 
     * NOTE: We don't try to kill the actual job on the remote system, we
     *       just clean up our persistent and in-memory data structures.
     *       Jobs that are queued or running on the remote system probably
     *       won't get blocked, so there's no point in trying to kill them.
     *       If this approach becomes undesirable, see AbstractJobKiller
     *       for ideas on how to kill a job.
     * 
     * @param cancelMsg asynchronous message to cancel a job's recovery 
     * @return true if recovery is cancelled, false otherwise
     */
    public synchronized boolean cancelRecovery(JobCancelRecoverMsg cancelMsg)
    {
        // Check that the job is in recovery.
        if (cancelMsg == null) return false;
        JobRecovery jobRecovery = _jobUuidIndex.get(cancelMsg.jobUuid);
        if (jobRecovery == null) {
            // The job is not in recovery
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_JOB_NOT_FOUND", cancelMsg.jobUuid);
            _log.info(msg);
            return false; 
        }
        
        // Get the job's blocked object.
        JobBlocked blockedJob = null;
        for (JobBlocked curJob : jobRecovery.getBlockedJobs()) 
            if (cancelMsg.jobUuid.equals(curJob.getJobUuid())) {
                blockedJob = curJob;
                break;
            }
        
        // Is the job in recovery?
        if (blockedJob == null) {
            // The job is not in recovery even though its indexed. This should never happen.
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_JOB_NOT_FOUND", cancelMsg.jobUuid);
            _log.error(msg);
            return false;
        }
        
        // Make sure we transition to a terminal state.
        if (!JobStatusType.isTerminal(cancelMsg.newStatus)) 
            cancelMsg.newStatus = JobStatusType.CANCELLED;
        
        // Make sure we have some sort of message to persist.
        if (StringUtils.isBlank(cancelMsg.statusMessage))
            cancelMsg.statusMessage = "Recovery cancelled at user request.";
        
        // Change the status of the job.
        boolean result = true;
        try {_jobsDao.setStatus(blockedJob.getJobUuid(), cancelMsg.newStatus, cancelMsg.statusMessage);}
        catch (JobException e) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_CANCEL_JOB_ERROR", jobRecovery.getId(), 
                                         blockedJob.getJobUuid(), e.getMessage());
            _log.error(msg, e);
            result = false;
        } 

        // Always remove the blocked job from our data structures.
        // Note that the recovery record is not null at this point.
        deleteBlockedJob(blockedJob.getJobUuid(), jobRecovery);
        
        // The job was in recovery and is not now.
        return result;
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initialize:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Initialize data structures that contain all the blocked jobs and 
     * recovery information needed to unblock those jobs.
     * 
     * @throws JobException if initialization fails
     */
    private void initialize() 
     throws JobException
    {
        // Read all recovery wait records from the database.
        // This should not be a memory problem since we don't
        // expect an inordinate number of jobs to be in the 
        // wait state at any moment.  If this assumption turns
        // out not to be true, we'll have to come up with 
        // something more sophisticated.
        List<JobRecovery> list = null;
        try {list = _recoveryDao.getRecoveryJobs();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_QUERY_ERROR", "recovery", 
                                             e.getMessage());
                _log.error(msg, e);
                throw new JobException(msg, e);
            }
        
        // Fill in the specific blocked job information in each recovery object 
        // and then add the recovery object to our recovery set and indices. If
        // there's a failure on any specific recovery record, that record's skipped
        // and processing continues.
        long curRecoveryId = 0;
        for (JobRecovery jobRecovery : list) 
        {
            // Squirrel away the id for use in error message.
            curRecoveryId = jobRecovery.getId();
            try {
                // Populate the blocked job list for the recovery job. An index on the
                // aloe_jobs_blocked prevents a job from appearing more than once in the table.
                List<JobBlocked> blockedJobs = _recoveryDao.getBlockedJobs(curRecoveryId);
                jobRecovery.addBlockedJobs(blockedJobs);
                
                // Record this recovery job in the manager's fields.
                addRecoveryJob(jobRecovery);
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_SELECT_BLOCKED_ERROR", curRecoveryId, 
                                             e.getMessage());
                _log.error(msg, e);
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* doRecover:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Run the tester to see if the blocking condition has cleared and then
     * either resubmit or reblock the jobs.
     *  
     * @param jobRecovery
     */
    private void doRecover(JobRecovery jobRecovery)
    {
        // Select the tester program based on the tester type.
        RecoverTester tester = null;
        try {
            // Get a tester class instance based on the type of test required.
            tester = jobRecovery.getTester();
        } catch (Exception e) {
            // Log the error.
            _log.error(makeInvalidJobMsg(jobRecovery, e), e);
            
            // Fail all blocked jobs in this recovery record. By not placing
            // the record back in the recovery set it is discarded.
            failAllBlockedJobs(jobRecovery, e.getMessage());
            return;
        }
        
        // Execute the test using the recovery test parameters.
        int unblockCount;
        try {unblockCount = tester.canUnblock(jobRecovery.getTesterParameters());}
            catch (Exception e) {
                // Log the error.
                _log.error(makeInvalidJobMsg(jobRecovery, e), e);
                
                // Fail all blocked jobs in this recovery record. By not placing
                // the record back in the recovery set it is discarded.
                failAllBlockedJobs(jobRecovery, e.getMessage());
                return;
            }
        
        // Resubmit the user job(s) if the blocking condition has cleared
        // or retain the recovery job if the user job(s) is still blocked.
        // If resubmission is attempted, the recovery record will be deleted.
        if (unblockCount <= 0) reblockUserJobs(jobRecovery);
         else resubmitUserJobs(jobRecovery, unblockCount);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addRecoveryJob:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Add the recovery job to our next-attempt-time sorted set.  We want to
     * make sure that (1) jobs are coalesced with others blocked on the same
     * condition and (2) no job appears more than once in the set.  We also
     * guarentee that no new job can be added unless it has a valid id.  
     * 
     * @param jobRecovery
     */
    private void addRecoveryJob(JobRecovery jobRecovery)
    {
        // Is there any thing to add?
        if (jobRecovery == null) return;
        
        // Firewall against incomplete recovery objects.  We make sure there is NO WAY that
        // a recovery job object can be placed in the internal set without a valid id.
        if (jobRecovery.getId() <= 0) {
            String msg = MsgUtils.getMsg("JOBS_BAD_RECOVERY_ID", jobRecovery.getTenantId(), 
                                         jobRecovery.getConditionCode().name(), 
                                         jobRecovery.getTesterHash(), jobRecovery.getId());
            _log.error(msg);
            return;
        }
        
        // Ignore any job that is already referenced by some recovery job. This
        // shouldn't happen since once a job is blocked it does not have any
        // opportunity to be blocked again.  We make note and move on if this
        // invariant is violated.
        HashSet<String> newJobUuids = new HashSet<>();
        List<JobBlocked> blockedJobs = jobRecovery.getBlockedJobs();
        ListIterator<JobBlocked> it = blockedJobs.listIterator();
        while (it.hasNext()) {
            // Remove any job that's already recovering.
            JobBlocked blockedJob = it.next();
            if (_jobUuidIndex.containsKey(blockedJob.getJobUuid()) || 
                newJobUuids.contains(blockedJob.getJobUuid())) 
            {
                it.remove();
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_DUPLICATE_JOB", 
                                             blockedJob.getJobUuid(), 
                                             jobRecovery.getId(),
                                             jobRecovery.getConditionCode().name());
                _log.warn(msg);
            } else {
                // Add the ones we are going to keep in our local set
                // as a guard against duplicate in the input recovery job.
                newJobUuids.add(blockedJob.getJobUuid());
            }
        }
        
        // Is there anything to recover?
        if (blockedJobs.isEmpty()) return;
        
        // Determine if other jobs are already blocked on the same condition.
        JobRecovery existingJobRecovery = _testerHashIndex.get(jobRecovery.getTesterHash());
        if (existingJobRecovery == null) {
            // Add the new job recovery object to the set.
            _recoveryJobs.add(jobRecovery);
            
            // Update the indices for the new recovery record and each of its blocked jobs.
            _testerHashIndex.put(jobRecovery.getTesterHash(), jobRecovery);
            for (JobBlocked blockedJob : jobRecovery.getBlockedJobs())
                _jobUuidIndex.put(blockedJob.getJobUuid(), jobRecovery);
        }
        else {
            // Add the jobs to the existing recovery object's blocked list.
            // Note that duplicates have already been removed, so it's safe
            // to just add the jobs to the existing recovery record's list.
            for (JobBlocked blockedJob : jobRecovery.getBlockedJobs()) {
                existingJobRecovery.addBlockedJob(blockedJob);
                
                // Add the blocked job to the index.
                _jobUuidIndex.put(blockedJob.getJobUuid(), existingJobRecovery);
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* deleteBlockedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Delete a blocked job from its recovery job records.  If the record
     * contains no more blocked jobs, delete it.  All indices associated with 
     * the blocked job and, if necessary, the recovery job record are also
     * cleaned up.
     * 
     * @param jobUuid the job that is no longer recovering
     */
    private void deleteBlockedJob(String jobUuid, JobRecovery jobRecovery)
    {
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DELETING_BLOCKED_JOB", jobUuid);
            _log.debug(msg);
        }
        
        // The recovery record should not be null.
        if (jobRecovery == null) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_NULL_RECOVERY_RECORD", jobUuid);
            _log.error(msg);
            return;
        }
        
        // Delete the blocked job record.
        List<JobBlocked> blockedJobs = jobRecovery.getBlockedJobs();
        ListIterator<JobBlocked> it = blockedJobs.listIterator();
        while (it.hasNext()) {
            JobBlocked blockedJob = it.next();
            if (jobUuid.equals(blockedJob.getJobUuid())) {
                it.remove();
                break;
            }
        }
        
        // Delete the job uuid index.
        _jobUuidIndex.remove(jobUuid);
        
        // Delete the persistent blocked job record.
        try {_recoveryDao.deleteBlockedJob(jobUuid);}
        catch (Exception e) {} // already logged
        
        // Remove the whole recovery record if no jobs are associated with it.
        if (blockedJobs.isEmpty()) deleteJobRecovery(jobRecovery);
    }
    
    /* ---------------------------------------------------------------------- */
    /* deleteJobRecovery:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Delete all references to a recovery record other than this method's 
     * parameter reference.  Basically, this means deleting all references to
     * to the record from the _jobUuidIndex map and all such references in the
     * _testerHashIndex.  
     * 
     * We assume that the jobRecovery object is not in the _recoveryJobs set
     * when this method is called. This assumption means the record will be 
     * garbage collected after this method completes and no other references
     * to it are left on the call stack.
     * 
     * It's imperative to clear any blocked jobs from the blocked jobs array
     * so that the recovery record is not retained by high-level recover()
     * method.
     * 
     * @param jobRecovery the record to delete
     */
    private void deleteJobRecovery(JobRecovery jobRecovery)
    {
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_DELETING_RECOVERY_RECORD", jobRecovery.getId(), jobRecovery.getTenantId());
            _log.debug(msg);
        }
        
        // Delete all index mappings from jobUuid to recovery record.
        for (JobBlocked blockedJob : jobRecovery.getBlockedJobs())
            _jobUuidIndex.remove(blockedJob.getJobUuid());
        
        // Delete the index mapping from tester hash to recovery record.
        _testerHashIndex.remove(jobRecovery.getTesterHash());
        
        // Delete the persistent recovery records and its blocked job records.
        try {_recoveryDao.deleteJobRecovery(jobRecovery.getId(), jobRecovery.getTenantId());}
        catch (Exception e) {
            List<String> blockedUuids = 
                jobRecovery.getBlockedJobs().stream().map(JobBlocked::getJobUuid).collect(Collectors.toList());
            String s = StringUtils.join(blockedUuids, ", ");
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_INVALID_JOB", jobRecovery.getId(),
                                         jobRecovery.getTenantId(), s, e.getMessage());
            _log.error(msg, e);
        }
        
        // Clear all block jobs so the record cannot be resurrected
        // in the top-level recover() method.
        jobRecovery.getBlockedJobs().clear();
    }
    
    /* ---------------------------------------------------------------------- */
    /* reblockUserJobs:                                                       */
    /* ---------------------------------------------------------------------- */
    /** This method is called when the blocking condition has not cleared up
     * and the job recovery record needs to be re-inserted in the recovery set.
     * We first need to calculate the next attempt time.  If that calculation
     * fails, the job recovery process has expired and all jobs blocked on the
     * condition are put in the failed state.
     * 
     * The updated attempt information is saved to the recovery table so that
     * recovery can pick up where it left off if a restart occurs.  If the 
     * database record cannot be updated, recovery continues with the idea that
     * future updates will resynchronize the persistent and in-memory data.
     * 
     * @param jobRecovery the recovery record to be re-inserted in the recovery set
     */
    private void reblockUserJobs(JobRecovery jobRecovery)
    {
        // Update the attempt information in the recovery object.
        try {jobRecovery.incrementAttempts();}
            catch (JobRecoveryExpiredException e) {
                // The job recovery method already logged the expiration,
                // so we only have to do the cleanup.
                failAllBlockedJobs(jobRecovery, e.getMessage());
                return;
            }
        
        // Persist the updated attempt information.  We ignore failures
        // to update the database and continue with recovery anyway.
        try {_recoveryDao.updateAttempts(jobRecovery);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INGORE_UPDATE_ERROR",
                                             jobRecovery.getId(), jobRecovery.getTenantId());
                _log.warn(msg, e);
            } 
        
        // Add the updated recovery record to the next-attempt-time sorted set.
        _recoveryJobs.add(jobRecovery);
    }
    
    /* ---------------------------------------------------------------------- */
    /* resubmitUserJobs:                                                      */
    /* ---------------------------------------------------------------------- */
    /** An attempt is made to resubmit each job in the blocked job list of the 
     * recovery record. Whether resubmission succeeds or not, all blocked jobs
     * exit recovery processing after this method completes.  In fact, the job
     * recovery record itself is available for garbage collection after this 
     * method executes.
     * 
     * Zombie Warning
     * --------------
     * Note that jobs can be left in inconsistent states if one or more of the
     * database or messaging system calls fails during this method's execution.
     * The issues are basically the same as those discussed in 
     * TenantQueueProcessor.putJobIntoRecovery(); see that method for details.
     * 
     * @param jobRecovery the record the will be deleted after its jobs are resubmitted
     */
    private void resubmitUserJobs(JobRecovery jobRecovery, int unblockCount)
    {
        // ---------------------- Set Up -----------------------------
        // Get the total number of job blocked in this recovery record.
        int totalBlockedJobs = jobRecovery.getBlockedJobs().size();
        
        // Calculate the number of jobs to actually resubmit.
        if (unblockCount < 0) unblockCount = 0; // safety
        int resubmitCount = Math.min(totalBlockedJobs, unblockCount);
        
        // Keep a list of all job uuids that are processed for clean up purposes.
        ArrayList<String> processedList = new ArrayList<>(resubmitCount);
        
        // Some debugging.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_RESUBMIT_COUNT", resubmitCount, totalBlockedJobs,
                                         jobRecovery.getId(), jobRecovery.getTenantId());
            _log.debug(msg);
        }
        
        // ---------------------- Resubmit Jobs ----------------------
        // Update the status and then resubmit each job.
        for (int i = 0; i < resubmitCount; i++)
        {
            // Get the next job to resubmit.
            JobBlocked blockedJob = jobRecovery.getBlockedJobs().get(i);
            
            // Record all jobs selected for resubmission so that the internal data
            // structure can be cleaned up when this loop completes. Whether jobs
            // are successfully resubmitted or not, they will always be cleaned up.
            processedList.add(blockedJob.getJobUuid());
            
            // Create the resubmission message.
            String message = MsgUtils.getMsg("JOBS_RECOVERY_RESUBMIT_JOB",
                                             jobRecovery.getId(), blockedJob.getJobUuid(),
                                             blockedJob.getSuccessStatus());
            if (_log.isDebugEnabled()) _log.debug(message);
            
            // Change the status of the job.
            try {_jobsDao.setStatus(blockedJob.getJobUuid(), blockedJob.getSuccessStatus(), message);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_RESUBMIT_JOB_ERROR", jobRecovery.getId(), 
                                             blockedJob.getJobUuid(), e.getMessage());
                _log.error(msg, e);
                
                // Fail the job.
                String name = getClass().getSimpleName();
                failJobStatus(name, blockedJob.getJobUuid(), jobRecovery.getTenantId(), msg);
                continue;
            } 
            
            /* ----------------------------------------------------
             * Catastrophic failure here leads to an inconsistency:
             * 
             *  - A non-blocked job is never placed on a submission
             *    queue; its state will never change.
             * ----------------------------------------------------
             */
            
            // Retrieve the job.
            Job job = null;
            try {job = _jobsDao.getJobByUUID(blockedJob.getJobUuid(), true);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_RESUBMIT_JOB_ERROR", jobRecovery.getId(), 
                                             blockedJob.getJobUuid(), e.getMessage());
                _log.error(msg, e);

                // Fail the job as long as it might exist.
                if (!(e instanceof TapisNotFoundException)) {
                    String name = getClass().getSimpleName();
                    failJobStatus(name, blockedJob.getJobUuid(), jobRecovery.getTenantId(), msg);
                }
                continue;
            }
            
            // Queue the job on its original tenant queue.
            try {JobQueueManager.getInstance().queueJob(job);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_RESUBMIT_JOB_ERROR", jobRecovery.getId(), 
                                             blockedJob.getJobUuid(), e.getMessage());
                _log.error(msg, e);
    
                // Fail the job.
                String name = getClass().getSimpleName();
                failJobStatus(name, blockedJob.getJobUuid(), jobRecovery.getTenantId(), msg);
            }
        }
        
        // ---------------------- Clean Up ---------------------------
        // Remove the resubmitted jobs from our data structures and the database.
        // As an optimization, we can delete the whole recovery record if the all
        // blocked jobs were processed (resubmitted or failed).
        if (resubmitCount == totalBlockedJobs) deleteJobRecovery(jobRecovery);
          else {
             // Delete each individual blocked job that was processed.
             for (String jobUuid : processedList) deleteBlockedJob(jobUuid, jobRecovery);
          }
    }
    
    /* ---------------------------------------------------------------------- */
    /* failAllBlockedJobs:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Transition all blocked jobs in the recovery record to the failed state.
     * 
     * @param jobRecovery the recovery record that is being discarded
     * @param message the failure message to be persisted
     */
    private void failAllBlockedJobs(JobRecovery jobRecovery, String message)
    {
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_FAILING_ALL_BLOCKED_JOBS", jobRecovery.getBlockedJobs().size(),
                                         jobRecovery.getId(), jobRecovery.getTenantId());
            _log.debug(msg);
        }
        
        // Change the status of each job to failed.
        String name = getClass().getSimpleName();
        for (JobBlocked blockedJob : jobRecovery.getBlockedJobs())
            failJobStatus(name, blockedJob.getJobUuid(), jobRecovery.getTenantId(), message);
        
        // Delete the recovery record.
        deleteJobRecovery(jobRecovery);
    }
    
    /* ---------------------------------------------------------------------- */
    /* failJobStatus:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Fail a single user job without throwing any exceptions by updating
     * the job record in the aloe_jobs table.
     * 
     * @param name the component issueing this call
     * @param jobUuid the job to fail
     * @param message message the failure message to be persisted
     */
    private void failJobStatus(String name, String jobUuid, String tenantId, String message)
    {
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_FAILING_JOBS", jobUuid);
            _log.debug(msg);
        }
        
        // Fail each job.
        try {_jobsDao.failJob(name, jobUuid, tenantId, message);} 
            catch (Exception e) {
                // Swallow exception.
                String msg = MsgUtils.getMsg("JOBS_STATUS_CHANGE_ERROR", 
                                             jobUuid, JobStatusType.FAILED.name());
                _log.error(msg, e);
            }
    }

    /* ---------------------------------------------------------------------- */
    /* makeInvalidJobMsg:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Make an error message here to reduce clutter. */
    private String makeInvalidJobMsg(JobRecovery jobRecovery, Exception e)
    {
        // Concatenate all blocked job uuids into a string for the message. 
        List<String> blockedUuids = 
            jobRecovery.getBlockedJobs().stream().map(JobBlocked::getJobUuid).collect(Collectors.toList());
        String s = StringUtils.join(blockedUuids, ", ");
        String msg = MsgUtils.getMsg("JOBS_RECOVERY_INVALID_JOB", jobRecovery.getId(),
                                     jobRecovery.getTenantId(), s, e.getMessage());
        return msg;
    }
}

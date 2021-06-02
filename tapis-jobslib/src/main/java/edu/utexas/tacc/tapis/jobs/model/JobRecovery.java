package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobInputException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryExpiredException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoverConditionCode;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicy;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyFactory;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTester;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTesterFactory;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTesterType;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** Java bean whose non-transient fields represent a job_recovery database record.
 * 
 * @author rcardone
 */
public final class JobRecovery 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobRecovery.class);
    
    private static final long MIN_WAIT_MS = 5000;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // It's probably overkill to make the id field volatile, but since one thread
    // sets the id field and all other threads depend on it, better safe than sorry.
    private volatile int               id;               // Unique id of the wait record
    private String                     tenantId;         // Tenant of this record's jobs
    private RecoverConditionCode       conditionCode;    // Reason for waiting
    private RecoverTesterType          testerType;       // Condition tester name
    private TreeMap<String,String>     testerParameters; // Tester constructor parms
    private String                     testerHash;       // Hash of tester type and parms
    private RecoverPolicyType          policyType;       // Next attempt policy type
    private TreeMap<String,String>     policyParameters; // Policy constructor parms
    
    private Instant                    created;          // Time wait record was defined
    private Instant                    lastUpdated;      // Time wait record was last updated
    private Instant                    nextAttempt;      // Next attempt time
    private int                        numAttempts;      // Number of unsuccessful attempts
    
    // These fields are not stored in the database.  To correctly 
    // initialize, use getters for these fields ALL the time.
    private transient RecoverTester    tester;           // Condition tester object
    private transient RecoverPolicy    policy;           // Next attempt policy object
    
    // Jobs blocked on this recovery's condition.
    private transient final List<JobBlocked> blockedJobs = new ArrayList<JobBlocked>();

    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Constructor used only to instantiate objects from database records.  */
    public JobRecovery() {}
    
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Constructor used to create new recovery records for database insertion.
     * 
     * @param recoverMsg the message received on the recovery queue
     * @throws JobInputException if validation fails
     */
    public JobRecovery(JobRecoverMsg recoverMsg)
     throws JobInputException
    {
        // Make sure we have some input.
        if (recoverMsg == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecovery", "recoveryMsg");
            _log.error(msg);
            throw new JobInputException(msg);
        }
        
        // Make sure the input validates.
        try {recoverMsg.validate();}
        catch (JobInputException e) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "JobRecovery", "recoveryMsg",
                                         TapisGsonUtils.getGson().toJson(recoverMsg));
            _log.error(msg, e);
            throw new JobInputException(msg, e);
        }
        
        // Extract all fields from the message.
        tenantId = recoverMsg.getTenantId();
        conditionCode = recoverMsg.getConditionCode();
        testerType = recoverMsg.getTesterType();
        testerParameters = recoverMsg.getTesterParameters();
        testerHash = recoverMsg.getTesterHash();
        policyType = recoverMsg.getPolicyType();
        policyParameters = recoverMsg.getPolicyParameters();
        
        // Get current timestamp.
        Instant now = Instant.now();
        created = now;
        lastUpdated = now;
        
        // Add this job's particulars.
        addBlockedJobInfo(recoverMsg.getJobUuid(), recoverMsg.getStatusMessage(), 
                          recoverMsg.getSuccessStatus(), now);
        
        // Get the next attempt time from the policy.  The time must be greater than 
        // or equal to zero, if it's not validate() will fail on a null nextAttempt.
        // This is the first time the policy is referenced, so a new policy object
        // is created and initialized.
        Long waitMillis = getPolicy().millisToWait();
        if (waitMillis != null) nextAttempt = now.plusMillis(Math.max(0, waitMillis));
    }

    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addBlockedJobs:                                                        */
    /* ---------------------------------------------------------------------- */
    /** The caller must guarantee that no duplicates are added.
     * 
     * @param newBlockedJobs the list of new jobs to be blocked
     */
    public void addBlockedJobs(List<JobBlocked> newBlockedJobs) 
    {
        // Add the job to the blocked list.
        blockedJobs.addAll(newBlockedJobs);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addBlockedJob:                                                         */
    /* ---------------------------------------------------------------------- */
    /** The caller must guarantee that no duplicates are added.
     * 
     * @param blocked the new blocked job.
     */
    public void addBlockedJob(JobBlocked blocked) 
    {
        // Add the job to the blocked list.
        blockedJobs.add(blocked);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addBlockedJobInfo:                                                     */
    /* ---------------------------------------------------------------------- */
    /** The caller must guarantee that no duplicates are added.  A blocked job
     * object is created and added to this objects blocked job list.
     * 
     * @param jobUuid
     * @param statusMessage
     * @param successStatus
     * @param created
     */
    public void addBlockedJobInfo(String jobUuid, String statusMessage, 
                                  JobStatusType successStatus, Instant created) 
    {
        // Create the new blocked object for this job.
        JobBlocked blocked = new JobBlocked();
        blocked.setJobUuid(jobUuid);
        blocked.setStatusMessage(statusMessage);
        blocked.setSuccessStatus(successStatus);
        blocked.setCreated(created);
        
        // Add the job to the blocked list.
        blockedJobs.add(blocked);
    }
    
    /* ---------------------------------------------------------------------- */
    /* incrementAttempts:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Increment the number of attempt and pass that number to the policy 
     * in a request to calculate wait time.  If the wait time is non-null,
     * the next attempt time is assigned.  Otherwise, the policy has determined
     * that this recovery has expired and the expired exception is thrown. 
     *  
     * The numAttempts and nextAttempt fields can be updated by this method.
     * It's the caller's responsibility to persist these updates to the database.
     */
    public void incrementAttempts()
     throws JobRecoveryExpiredException
    {
        // Increment the number of attempts and use that to calculate the next 
        // attempt time.  If null is returned, we expect that the policy has
        // already logged the precise condition that prevents the wait time
        // calculation.  We log additional job recovery information here.
        ++numAttempts;
        Long waitMillis = getPolicy().millisToWait();
        if (waitMillis == null) {
            // Get the list of blocked job uuids.
            List<String> blockedUuids = 
                blockedJobs.stream().map(JobBlocked::getJobUuid).collect(Collectors.toList());
            String s = StringUtils.join(blockedUuids, ", ");
            
            // Throw exception with a detailed message.
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_EXPIRED", id, tenantId,
                                         conditionCode.name(), policyType.name(),
                                         testerType.name(), numAttempts-1, 
                                         getPolicy().getReasonCode().name(), s);
            _log.warn(msg);
            throw new JobRecoveryExpiredException(msg);
        }
        
        // Assign the next attempt time.
        nextAttempt = Instant.now().plusMillis(Math.max(MIN_WAIT_MS, waitMillis));
    }
    
    /* ---------------------------------------------------------------------- */
    /* validate:                                                              */
    /* ---------------------------------------------------------------------- */
    /** The id field is not checked as it will be assigned at database insertion
     * time.
     * 
     * @throws JobInputException if invalid field values are detected
     */
    public void validate() 
     throws JobInputException
    {
        // Many null checks.
        if (tenantId == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "tenantId");
            throw new JobInputException(msg);
        }
        if (conditionCode == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "conditionCode");
            throw new JobInputException(msg);
        }
        if (testerType == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "testerType");
            throw new JobInputException(msg);
        }
        if (testerParameters == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "testerParameters");
            throw new JobInputException(msg);
        }
        if (testerHash == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "testerHash");
            throw new JobInputException(msg);
        }
        if (policyType == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "policyType");
            throw new JobInputException(msg);
        }
        if (policyParameters == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "policyParameters");
            throw new JobInputException(msg);
        }
        if (created == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "created");
            throw new JobInputException(msg);
        }
        if (lastUpdated == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "lastUpdated");
            throw new JobInputException(msg);
        }
        if (nextAttempt == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "nextAttempt");
            throw new JobInputException(msg);
        }
        
        // Make sure there's at least one job blocked record.
        if (blockedJobs.isEmpty()) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validate", "blockedJobs.isEmpty", "true");
            throw new JobInputException(msg);
        }
        
        // Now check each blocked job record.
        for (JobBlocked blocked: blockedJobs) blocked.validate();
    }
    
    /* ---------------------------------------------------------------------- */
    /* equals:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public boolean equals(Object obj)
    {
        // One of us?
        if (!(obj instanceof JobRecovery)) return false;
        JobRecovery that = (JobRecovery) obj;
        
        // Equality doesn't hold up in partially formed objects.
        if ((this.tenantId == null)   || (that.tenantId == null) ||
            (this.testerHash == null) || (that.testerHash == null))
          return false;
        
        // The tenant and tester hash determine equality.  Though not strictly
        // necessary, we explicitly compare tenant ids even though that id
        // is incorporated in the hash value.  See JobRecoveryMsg.setTesterInfo()
        // for details.
        if (this.tenantId.equals(that.tenantId) && 
            this.testerHash.equals(that.testerHash))
          return true;
        
        return false;
    }

    /* ---------------------------------------------------------------------- */
    /* hashCode:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public int hashCode()
    {
        // Conform to equals().
        StringBuilder buf = new StringBuilder();
        buf.append((tenantId == null)   ? "null" : tenantId);
        buf.append("+");
        buf.append((testerHash == null) ? "null" : testerHash);
        return buf.toString().hashCode();
    }

    /* ---------------------------------------------------------------------- */
    /* toString:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public String toString() {return TapisUtils.toString(this);}
    
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    // Always use this method to get a non-null policy,
    // initializing the policy field when necessary.
    public RecoverPolicy getPolicy() 
    {
        if (policy == null)
            policy = RecoverPolicyFactory.getInstance().getPolicy(this);
        return policy;
    }
    
    // Always use this method to get a non-null policy,
    // initializing the tester field when necessary.
    public RecoverTester getTester() 
    {
        if (tester == null)
            tester = RecoverTesterFactory.getInstance().getTester(this);
        return tester;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public RecoverConditionCode getConditionCode() {
        return conditionCode;
    }

    public void setConditionCode(RecoverConditionCode conditionCode) {
        this.conditionCode = conditionCode;
    }

    public RecoverTesterType getTesterType() {
        return testerType;
    }

    public void setTesterType(RecoverTesterType testerType) {
        this.testerType = testerType;
    }
    
    public String getTesterHash() {
        return testerHash;
    }

    public void setTesterHash(String testerHash) {
        this.testerHash = testerHash;
    }

    public TreeMap<String, String> getTesterParameters() {
        return testerParameters;
    }

    public void setTesterParameters(TreeMap<String, String> testerParameters) {
        this.testerParameters = testerParameters;
    }

    public RecoverPolicyType getPolicyType() {
        return policyType;
    }

    public void setPolicyType(RecoverPolicyType policyType) {
        this.policyType = policyType;
    }

    public TreeMap<String, String> getPolicyParameters() {
        return policyParameters;
    }

    public void setPolicyParameters(TreeMap<String, String> policyParameters) {
        this.policyParameters = policyParameters;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Instant getNextAttempt() {
        return nextAttempt;
    }

    public void setNextAttempt(Instant nextAttempt) {
        this.nextAttempt = nextAttempt;
    }

    public int getNumAttempts() {
        return numAttempts;
    }

    public void setNumAttempts(int numAttempts) {
        this.numAttempts = numAttempts;
    }

    public void setTester(RecoverTester tester) {
        this.tester = tester;
    }

    public void setPolicy(RecoverPolicy policy) {
        this.policy = policy;
    }

    public List<JobBlocked> getBlockedJobs() {
        return blockedJobs;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /* ********************************************************************** */
    /*                     NextAttemptComparator Class                        */
    /* ********************************************************************** */
    public static final class NextAttemptComparator
     implements Comparator<JobRecovery>
    {
        /** This method assumes only recovery records from the same tenant
         * will be compared.  This assumption is guaranteed by including the
         * tenant id in the hash itself.  See Comparator documentation for 
         * details about being consistent with equals.  
         */
        @Override
        public int compare(JobRecovery rec1, JobRecovery rec2) 
        {
            // Recovery objects are considered equal if that have the same 
            // tester hash and exist in the same tenant.  The standard
            // JobRecovery constructor takes a message parameter and 
            // guarantees non-null values for the fields we inspect during 
            // this comparison.  (The no-arg constructor is only used for 
            // marshalling objects by gson.)
            //
            // That said, we still guard against blowing up.
            if ((rec1 == null)             || (rec2 == null)            || 
                (rec1.testerHash == null)  || (rec2.testerHash == null) ||
                (rec1.nextAttempt == null) || (rec2.nextAttempt == null))
              {
                // This should never happen.
                String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "compare", "testerHash/nextAttempt");
                _log.error(msg);
                return -1;  // We have to return something...
              }
            
            // Assume the tenant ids are the same and just compare the hashes.
            if (rec1.testerHash.equals(rec2.testerHash)) return 0;
            
            // The two records have different hashes, so we use the next attempt
            // time to determine their relative ordering.
            if (rec1.nextAttempt.isBefore(rec2.nextAttempt)) return -1;
              else if (rec1.nextAttempt.isAfter(rec2.nextAttempt)) return 1;
              else return 0;
        }
    }
}

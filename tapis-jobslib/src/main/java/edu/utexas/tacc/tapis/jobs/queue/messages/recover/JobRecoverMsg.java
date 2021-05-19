package edu.utexas.tacc.tapis.jobs.queue.messages.recover;

import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobInputException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.recover.RecoverConditionCode;
import edu.utexas.tacc.tapis.jobs.recover.RecoverPolicyType;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTesterType;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** The recovery message used to populate the job_recovery and job_blocked 
 * tables.  This class calculates a hash of the tester type and parameters
 * to identify jobs that are blocked on the same condition.
 * 
 * @author rcardone
 */
public class JobRecoverMsg 
 extends RecoverMsg
{
    /* ********************************************************************** */
    /*                              Constructor                               */
    /* ********************************************************************** */
    // For testing only.
    public JobRecoverMsg() {super(RecoverMsgType.RECOVER, "UnknownSender");}
    
    // Use static create method.
    private JobRecoverMsg(String senderId) {super(RecoverMsgType.RECOVER, senderId);}
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // We use TreeMap as the concrete Map type for parameters so that the 
    // parameters maintain the same order during iteration.  This provides an
    // extra level of assurance when comparing parameters from different sources.
    //
    // The testerHash comprises the tenant, tester type and its parameters to uniquely
    // identify a condition on which one or more jobs may be blocked.  The strict
    // ordering of test parameter keys makes the hashes that incorporate those
    // parameters stable.
    //
    private  String                  jobUuid;          // Job in wait state
    private  String                  jobOwner;         // Job's owner
    private  String                  tenantId;         // Job's tenant ID
    private  RecoverConditionCode    conditionCode;    // Reason for waiting
    private  RecoverPolicyType       policyType;       // Next attempt policy 
    private  TreeMap<String,String>  policyParameters; // Policy parms
    private  JobStatusType           successStatus;    // New status for job after success
    private  String                  statusMessage;    // The message inserted in the job record
    
    // There's a single setter for these fields.
    private RecoverTesterType        testerType;       // Condition tester name
    private TreeMap<String,String>   testerParameters; // Tester parms
    private String                   testerHash;       // Hash of tenant, testerType & testerParameters
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* setTesterInfo:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Control access to the tester fields since we need to create a hash
     * from their values.  Each of the input fields are incorporated into a 
     * hash value used to group jobs blocked on the same condition.
     * 
     * Note that it is possible that two recovery messages with different condition
     * codes code have the same tester type and parameters.  In that case, the first
     * message's recovery record would take priority and all jobs would be blocked
     * under that record.  In practice this problem should not arise because
     * different conditions are expected to use different monitoring tests.  
     * 
     * @param tenant the job's tenant
     * @param type the tester type processor
     * @param parms parameters used by the test type processor
     */
    public void setTesterInfo(String tenant, RecoverTesterType type, TreeMap<String,String> parms)
    {
        // Don't allow a null type.
        if (type == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setTesterInfo", "type");
            throw new TapisRuntimeException(msg);
        }
        testerType = type;
        
        // Null parms are replaced with the empty string.
        if (parms == null) testerParameters = new TreeMap<>();
          else testerParameters = parms;
        
        // Calculate the hash for this combination of tester information.
        String data = tenant + "|" + type.name() + "|" + TapisGsonUtils.getGson().toJson(testerParameters);
        testerHash = DigestUtils.sha1Hex(data);
    }

    /* ---------------------------------------------------------------------- */
    /* validate:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Basic validation of all required fields.
     * 
     * @throws JobException on an invalid field value
     */
    public void validate()
     throws JobInputException
    {
        // Many null checks.
        if (jobUuid == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "jobUuid");
            throw new JobInputException(msg);
        }
        if (jobOwner == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "jobOwner");
            throw new JobInputException(msg);
        }
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
            String msg = MsgUtils.getMsg("TAPISE_NULL_PARAMETER", "validate", "policyType");
            throw new JobInputException(msg);
        }
        if (policyParameters == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "policyParameters");
            throw new JobInputException(msg);
        }
        if (successStatus == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "successStatus");
            throw new JobInputException(msg);
        }
        if (statusMessage == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "statusMessage");
            throw new JobInputException(msg);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* create:                                                                */
    /* ---------------------------------------------------------------------- */
    public static JobRecoverMsg create(Job job, 
                                       String senderId, 
                                       RecoverConditionCode conditionCode,
                                       RecoverPolicyType policyType, 
                                       TreeMap<String,String> policyParameters, 
                                       JobStatusType successStatus, 
                                       String statusMessage, 
                                       RecoverTesterType testerType, 
                                       TreeMap<String,String> testerParms)
    {
        // Create the new job recover message.
        JobRecoverMsg rmsg = new JobRecoverMsg(senderId);
        
        // Check input.
        if (job == null) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecoverMsg", "job"));
        if (StringUtils.isBlank(senderId)) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecoverMsg", "senderId"));
        if (conditionCode == null) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecoverMsg", "conditionCode"));
        if (policyType == null) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecoverMsg", "policyType"));
        if (successStatus == null) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecoverMsg", "successStatus"));
        if (StringUtils.isBlank(statusMessage)) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPISE_NULL_PARAMETER", "JobRecoverMsg", "statusMessage"));
        if (testerType == null) 
            throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobRecoverMsg", "testerType"));
        
        // Create empty policy parameters if necessary.
        // Null testerParms are handled later.
        if (policyParameters == null) policyParameters = new TreeMap<>();
        
        // Fill in the other message fields.
        rmsg.setJobUuid(job.getUuid());
        rmsg.setJobOwner(job.getOwner());
        rmsg.setTenantId(job.getTenant());
        rmsg.setConditionCode(conditionCode);
        rmsg.setPolicyType(policyType);
        rmsg.setPolicyParameters(policyParameters);
        rmsg.setSuccessStatus(successStatus);
        rmsg.setStatusMessage(statusMessage);
        rmsg.setTesterInfo(job.getTenant(), testerType, testerParms);
        
        return rmsg;
    }
    
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public RecoverTesterType getTesterType() {return testerType;}
    public TreeMap<String, String> getTesterParameters() {return testerParameters;}
    public String getTesterHash() {return testerHash;}

    public String getJobUuid() {
        return jobUuid;
    }

    public void setJobUuid(String jobUuid) {
        this.jobUuid = jobUuid;
    }

    public String getJobOwner() {
        return jobOwner;
    }

    public void setJobOwner(String jobOwner) {
        this.jobOwner = jobOwner;
    }
    
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public RecoverConditionCode getConditionCode() {
        return conditionCode;
    }

    public void setConditionCode(RecoverConditionCode conditionCode) {
        this.conditionCode = conditionCode;
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

    public JobStatusType getSuccessStatus() {
        return successStatus;
    }

    public void setSuccessStatus(JobStatusType successStatus) {
        this.successStatus = successStatus;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }
}

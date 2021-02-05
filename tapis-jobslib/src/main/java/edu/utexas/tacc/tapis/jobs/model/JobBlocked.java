package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.jobs.exceptions.JobInputException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Descriptor of a blocked job.
 * 
 * @author rcardone
 */
public final class JobBlocked 
{
    /* ********************************************************************** */
    /*                                Constants                               */
    /* ********************************************************************** */
    public static final int STATUS_MESSAGE_LEN = 2048;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private int                     id;               // Unique id of the wait record
    private int                     recoveryId;       // ID of the associated recovery record                  
    private String                  jobUuid;          // Job in wait state
    private JobStatusType           successStatus;    // New status for job after success
    private Instant                 created;          // Time wait record was defined
    private String                  statusMessage;    // Message also written to jobs table
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* validate:                                                              */
    /* ---------------------------------------------------------------------- */
    /** This method does not check the recovery id or its own id.  Those are
     * late bindings that are assigned during database insertion.
     * 
     * @throws JobInputException if invalid field values are detected
     */
    public void validate() 
     throws JobInputException
    {
        // Many null checks.
        if (jobUuid == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "jobUuid");
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
        if (created == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "created");
            throw new JobInputException(msg);
        }
    }
        
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public int getRecoveryId() {
        return recoveryId;
    }
    public void setRecoveryId(int recoveryId) {
        this.recoveryId = recoveryId;
    }
    public String getJobUuid() {
        return jobUuid;
    }
    public void setJobUuid(String jobUuid) {
        this.jobUuid = jobUuid;
    }
    public JobStatusType getSuccessStatus() {
        return successStatus;
    }
    public void setSuccessStatus(JobStatusType successStatus) {
        this.successStatus = successStatus;
    }
    public Instant getCreated() {
        return created;
    }
    public void setCreated(Instant created) {
        this.created = created;
    }
    public String getStatusMessage() {
        return statusMessage;
    }
    public void setStatusMessage(String statusMessage) {
        // Truncate status message if necessary.
        if ((statusMessage != null) && statusMessage.length() > STATUS_MESSAGE_LEN)
            statusMessage = statusMessage.substring(0, STATUS_MESSAGE_LEN - 1);
        this.statusMessage = statusMessage;
    }
}

package edu.utexas.tacc.tapis.jobs.queue.messages.recover;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;

/** Cancel the recovery of a job by removing it from the blocked table and
 * resetting its status message.  This is an administrative command that
 * overrides the normal recovery of a job.
 * 
 * @author rcardone
 */
public class JobCancelRecoverMsg 
 extends RecoverMsg
{
    /* ********************************************************************** */
    /*                              Constructor                               */
    /* ********************************************************************** */
    public JobCancelRecoverMsg() {super(RecoverMsgType.CANCEL_RECOVER, "UnknownSender");}
    public JobCancelRecoverMsg(String senderId) {super(RecoverMsgType.CANCEL_RECOVER, senderId);}
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    public  String                  jobUuid;          // Blocked job whose recovery will be cancelled
    public  String                  tenantId;         // Job's tenant ID
    public  JobStatusType           newStatus = JobStatusType.CANCELLED; // A terminal status for job 
    public  String                  statusMessage;    // The message inserted in the job record
}

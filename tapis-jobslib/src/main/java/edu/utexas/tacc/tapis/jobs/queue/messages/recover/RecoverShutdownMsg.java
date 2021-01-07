package edu.utexas.tacc.tapis.jobs.queue.messages.recover;

/** Cancel the recovery of a job by removing it from the blocked table and
 * resetting its status message.  This is an administrative command that
 * overrides the normal recovery of a job.
 * 
 * @author rcardone
 */
public class RecoverShutdownMsg 
 extends RecoverMsg
{
    // Fields.
    private boolean force = true;
    private String  tenantId;
    
    // Constructors.
    public RecoverShutdownMsg() {super(RecoverMsgType.RECOVER_SHUTDOWN, "UnknownSender");}
    public RecoverShutdownMsg(String senderId) {super(RecoverMsgType.RECOVER_SHUTDOWN, senderId);}

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}

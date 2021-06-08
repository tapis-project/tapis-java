package edu.utexas.tacc.tapis.jobs.monitors.parsers;

public enum SlurmStatusType 
{
   // These are the possible states returned by squeue and sacct.  squeue can return any of
   // them, sacct will only return a subset of them.
    
   BOOT_FAIL("BF","Job terminated due to launch failure, typically due to a hardware failure "
             + "(e.g. unable to boot the node or block and the job can not be requeued)."),
   
   CANCELLED("CA","Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated."),
   COMPLETED("CD","Job has terminated all processes on all nodes with an exit code of zero."),
   CONFIGURING("CF","Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting)."),
   COMPLETING("CG","Job is in the process of completing. Some processes on some nodes may still be active."),
   DEADLINE("DL","Job missed its deadline."),
   EQW("EQW", "Job started but there was an unrecoverable error. Job will remain in this unrecoverable state until manually cleaned up."),
   FAILED("F","Job terminated with non-zero exit code or other failure condition."),
   NODE_FAIL("NF","Job terminated due to failure of one or more allocated nodes."),
   OUT_OF_MEMORY("OOM", "Job experienced out of memory error."),
   PENDING("PD","Job is awaiting resource allocation. Note for a job to be selected in this "
           + "state it must have 'EligibleTime' in the requested time interval or different from "
           + "'Unknown'. The 'EligibleTime' is displayed by the 'scontrol show job' command. "
           + "For example jobs submitted with the '--hold' option will have 'EligibleTime=Unknown' "
           + "as they are pending indefinitely."),
   PREEMPTED("PR","Job terminated due to preemption."),
   RESV_DEL_HOLD("RD", "Job is held."),
   REQUEUE_FED("RF", "Job is being requeued by a federation."),
   REQUEUE_HOLD("RH", "Held job is being requeued."),
   REQUEUED("RQ", "Completing job is being requeued."),
   RUNNING("R","Job currently has an allocation."),
   RESIZING("RS","Job is about to change size."),
   REVOKED("RV", "Sibling was removed from cluster due to other cluster starting the job."),
   SPECIAL_EXIT("SE", "The job was requeued in a special state. This state can be set by users if the job has terminated with a particular exit value."),
   STAGE_OUT("SO", "Job is staging out files."),
   STOPPED("ST", "Job has an allocation, but execution has been stopped with SIGSTOP signal. CPUS have been retained by this job."),   
   SUSPENDED("S","Job has an allocation, but execution has been suspended."),
   TIMEOUT("TO","Job terminated upon reaching its time limit.");

   // Fields.
   private String description;
   private String code; 
   
   // Constructor.
   private SlurmStatusType(String code, String description) {
       this.setCode(code);
       this.setDescription(description);
   }
   
   // Accessors.
   public  String getDescription() {return description;}
   private void setDescription(String description) {this.description = description;}
   public  String getCode() {return code;}
   private void setCode(String code) {this.code = code;}
   
   @Override
   public String toString() {return name() + " - " + getDescription();}
   
   /** Waiting for allocation.
    */
   public boolean isQueued() {
       return this == PENDING;
   }
   
   /* ------------------ Status Classifier Methods ------------------ */
   public boolean isActive() {
       boolean activeStatus = false;
       switch (this) {
           case RUNNING:
           case RESIZING:
           case COMPLETING:
           case CONFIGURING:
           case REQUEUE_FED:
           case REQUEUE_HOLD:
           case REQUEUED:
           case STAGE_OUT:
               activeStatus = true;
           break;
       default:
           // anything else we assume it's not active.
       }
       return activeStatus;
   }
   
   public boolean isFailed() {
       boolean hasfailedStatus = false;
       switch (this) {
           case BOOT_FAIL:
           case CANCELLED:
           case DEADLINE:
           case FAILED:
           case NODE_FAIL:
           case OUT_OF_MEMORY:
           case TIMEOUT:
           case PREEMPTED:
           case REVOKED:
           case STOPPED:
               hasfailedStatus = true;
           break;
       default:
           // anything else we assume it's not failed.
       }
       return hasfailedStatus;
   }
   
   public boolean isPaused() {
       return this == SUSPENDED || this == RESV_DEL_HOLD;
   }
   
   public boolean isCompleted() {
       return this == COMPLETED || this == SPECIAL_EXIT;
   }
   
   public boolean isUnrecoverable() {
       return this == EQW;
   }
}
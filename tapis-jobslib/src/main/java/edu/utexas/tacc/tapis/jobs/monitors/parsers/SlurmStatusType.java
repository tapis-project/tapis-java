package edu.utexas.tacc.tapis.jobs.monitors.parsers;

public enum SlurmStatusType 
{
   // These are the possible states returned by squeue and sacct.  squeue can return any of
   // them, sacct will only return a subset of them.  Both short and long form are defined
   // to cover all bases.
    
   // Long form status types.
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
   TIMEOUT("TO","Job terminated upon reaching its time limit."),

   // Short form of the above status types.
   BF("BOOT_FAIL","Job terminated due to launch failure, typically due to a hardware failure "
           + "(e.g. unable to boot the node or block and the job can not be requeued)."),
   CA("CANCELLED","Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated."),
   CD("COMPLETED","Job has terminated all processes on all nodes with an exit code of zero."),
   CF("CONFIGURING","Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting)."),
   CG("COMPLETING","Job is in the process of completing. Some processes on some nodes may still be active."),
   DL("DEADLINE","Job missed its deadline."),
   F("FAILED","Job terminated with non-zero exit code or other failure condition."),
   NF("NODE_FAIL","Job terminated due to failure of one or more allocated nodes."),
   OOM("OUT_OF_MEMORY", "Job experienced out of memory error."),
   PD("PENDING","Job is awaiting resource allocation. Note for a job to be selected in this "
           + "state it must have 'EligibleTime' in the requested time interval or different from "
           + "'Unknown'. The 'EligibleTime' is displayed by the 'scontrol show job' command. "
           + "For example jobs submitted with the '--hold' option will have 'EligibleTime=Unknown' "
           + "as they are pending indefinitely."),
   PR("PREEMPTED","Job terminated due to preemption."),
   RD("RESV_DEL_HOLD", "Job is held."),
   RF("REQUEUE_FED", "Job is being requeued by a federation."),
   RH("REQUEUE_HOLD", "Held job is being requeued."),
   RQ("REQUEUED", "Completing job is being requeued."),
   R("RUNNING","Job currently has an allocation."),
   RS("RESIZING","Job is about to change size."),
   RV("REVOKED", "Sibling was removed from cluster due to other cluster starting the job."),
   SE("SPECIAL_EXIT", "The job was requeued in a special state. This state can be set by users if the job has terminated with a particular exit value."),
   SO("STAGE_OUT", "Job is staging out files."),
   ST("STOPPED", "Job has an allocation, but execution has been stopped with SIGSTOP signal. CPUS have been retained by this job."),   
   S("SUSPENDED","Job has an allocation, but execution has been suspended."),
   TO("TIMEOUT","Job terminated upon reaching its time limit.");
    
   
    
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
       return this == PENDING || this == PD;
   }
   
   /* ------------------ Status Classifier Methods ------------------ */
   public boolean isActive() {
       boolean activeStatus = false;
       switch (this) {
           case RUNNING:      case R:
           case RESIZING:     case RS:
           case COMPLETING:   case CG:
           case CONFIGURING:  case CF:
           case REQUEUE_FED:  case RF:
           case REQUEUE_HOLD: case RH:
           case REQUEUED:     case RQ:
           case STAGE_OUT:    case SO:
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
           case BOOT_FAIL:     case BF:
           case CANCELLED:     case CA:
           case DEADLINE:      case DL:
           case FAILED:        case F:
           case NODE_FAIL:     case NF:
           case OUT_OF_MEMORY: case OOM:
           case TIMEOUT:       case TO:
           case PREEMPTED:     case PR:
           case REVOKED:       case RV:
           case STOPPED:       case ST:
               hasfailedStatus = true;
           break;
       default:
           // anything else we assume it's not failed.
       }
       return hasfailedStatus;
   }
   
   public boolean isPaused() {
       return this == SUSPENDED     || this == S ||
              this == RESV_DEL_HOLD || this == RD;
   }
   
   public boolean isCompleted() {
       return this == COMPLETED    || this == CD ||
              this == SPECIAL_EXIT || this == SE;
   }
   
   public boolean isUnrecoverable() {
       return this == EQW;
   }
}
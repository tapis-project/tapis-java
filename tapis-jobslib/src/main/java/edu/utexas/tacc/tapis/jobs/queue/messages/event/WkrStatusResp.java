package edu.utexas.tacc.tapis.jobs.queue.messages.event;

//import edu.utexas.tacc.aloe.jobs.worker.JobWorkerParameters;

public final class WkrStatusResp 
 extends EventMsg
{
    /* ********************************************************************** */
    /*                              Constructor                               */
    /* ********************************************************************** */
    public WkrStatusResp() {super(EventType.WKR_STATUS_RESP);}

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Command line parameters.
//    public JobWorkerParameters workerParms;
    
    // Unique identifier of this worker instance.
    public String              workerUuid;
       
    // Counter used when naming threads.
    public int                 threadSeqNo;
    
    // The thread restart throttle limits the number 
    // of threads started within a time window.
    public int                 throttleWindowSeconds;
    public int                 throttleLimit;
    public int                 throttleQueueLength;
    
    // The thread group for all explicitly spawned worker threads in this program.
    public String              workerThreadGroupName;
    public int                 workerThreadGroupNumThreads;
    
    // The thread group for general topic thread in this program.
    public String              topicThreadGroupName;
    public int                 topicThreadGroupNumThreads;
    
    // The thread group for job-specific threads spawned by worker threads.
    public String              jobThreadGroupName;
    public int                 jobThreadGroupNumThreads;
    
    // Shutdown components.  
    public boolean             shuttingDown;      // Flag indicates shutdown
}

package edu.utexas.tacc.tapis.jobs.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

abstract class JobWorkerThread 
 extends Thread
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobWorkerThread.class);
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  private final String            _qname;     // the queue or topic that this thread reads
  private final AbstractProcessor _processor; // the message processor
  private final JobWorker         _worker;    // the top-level worker instance 
        
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  protected JobWorkerThread(ThreadGroup threadGroup, String threadName, 
                            JobWorker worker, String qname, AbstractProcessor processor) 
  {
      // Save input parameters.
      super(threadGroup, threadName);
      _worker = worker;
      _qname = qname;
      _processor = processor;
  }
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* run:                                                                   */
  /* ---------------------------------------------------------------------- */
  @Override
  public void run() {
    // Assign a base logging identifier to this thread before it calls
    // any request processor.  Long-lived request processors can reassign
    // this value on each request to demark the boundary between requests.
    MDC.put(TapisConstants.MDC_ID_KEY, TapisUtils.getRandomString());
      
    // Announce our arrival.
    if (_log.isInfoEnabled())
      _log.info(MsgUtils.getMsg("JOBS_THREAD_STARTING", 
                                Thread.currentThread().getName(),
                                Thread.currentThread().getId(),
                                _worker.getParms().name, _qname));
    
    // All the work takes place in the processor class.
    try {_processor.getNextMessage();}
    finally {
        // Give threads a chance to clean up before terminating.
        cleanUp(_processor, _worker, _qname);
    }
    
    // Announce our departure.
    if (_log.isInfoEnabled()) {
      String msg = "JOBS_THREAD_STOPPING";
      if (Thread.currentThread().isInterrupted()) msg = "JOBS_THREAD_INTERRUPTED";
      _log.info(MsgUtils.getMsg(msg, 
                                Thread.currentThread().getName(),
                                Thread.currentThread().getId(),
                                _worker.getParms().name, _qname));
    }
  }   
  
  /* ********************************************************************** */
  /*                           Protected Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* cleanUp:                                                               */
  /* ---------------------------------------------------------------------- */
  /** Allow subclasses to clean up as the thread is terminating.  By overriding
   * this method threads can clean up worker or job specific queue bindings.
   */
  protected void cleanUp(AbstractProcessor processor, JobWorker jobWorker, String qname) {}
}

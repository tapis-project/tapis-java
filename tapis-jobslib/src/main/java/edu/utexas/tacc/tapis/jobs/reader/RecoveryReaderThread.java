package edu.utexas.tacc.tapis.jobs.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.RecoverShutdownMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryManager;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class RecoveryReaderThread 
 extends Thread
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoveryReaderThread.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private final RecoveryReader _reader;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public RecoveryReaderThread(ThreadGroup threadGroup, String threadName,
                                RecoveryReader reader)
    {
        // Save inputs.
        super(threadGroup, threadName);
        _reader = reader;
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* run:                                                                   */
    /* ---------------------------------------------------------------------- */
    @Override
    public void run()
    {
        // Tracing.
        if (_log.isInfoEnabled()) 
            _log.info(MsgUtils.getMsg("JOBS_RECOVERY_THREAD_STARTING", 
                                      Thread.currentThread().getName(),
                                      Thread.currentThread().getId(),
                                      _reader.getName(), _reader.getQueueName()));
        
        // Create the recovery manager.
        RecoveryManager mgr = null;
        try {mgr = RecoveryManager.getInstance(_reader);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INIT_MGR_ERROR", 
                                             _reader.getName(), _reader.getQueueName());
                _log.error(msg, e);
                shutdownReader();
                return;
            }
        
        // The new recovery message read loop.
        while (true) {
            // Get the number of milliseconds to wait before 
            // performing a recovery action.
            long waitMillis = mgr.getMillisToWakeUp();
        
            // Wait on the queue for a recovery command.
            JobRecovery jobRecovery = null;
            try {
                // Check if we were interrupted.
                if (threadInterrupted()) break;
                
                // Log our upcoming wait time.
                if (_log.isInfoEnabled()) 
                    _log.info(MsgUtils.getMsg("TAPIS_WAIT", "RecoveryManager", 
                                              _reader.getQueueName(), waitMillis));
                
                // Timeouts return null.
                jobRecovery = _reader.pollRecoveryQueue(waitMillis);
            } 
            catch (InterruptedException e) {
                // End thread here.
                if (_log.isInfoEnabled()) {
                    String msg = MsgUtils.getMsg("JOBS_RECOVERY_THREAD_INTERRUPTED", 
                                                 Thread.currentThread().getName(),
                                                 Thread.currentThread().getId(),
                                                 _reader.getName(), _reader.getQueueName());
                    _log.info(msg);
                }
                break;
            }
            
            // Let the recovery manager incorporate the new recovery message
            // if one is returned and process all recovery actions that are due.
            // This thread executes its recovery task with exclusive database
            // access.
            synchronized (_reader.getDBLock()) {mgr.recover(jobRecovery);}
        }
        
        // Say goodbye.
        if (_log.isInfoEnabled()) 
            _log.info(MsgUtils.getMsg("JOBS_RECOVERY_THREAD_STOPPING", 
                                      Thread.currentThread().getName(),
                                      Thread.currentThread().getId(),
                                      _reader.getName(), _reader.getQueueName()));
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* threadInterrupted:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Check if the current thread was interrupted. 
     * @return true if the thread was interrupted, false otherwise
     * */
    private boolean threadInterrupted()
    {
        // Check if we were interrupted and clear the interrupted bit.
        if (Thread.interrupted()) {
            if (_log.isInfoEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_THREAD_INTERRUPTED", 
                                             Thread.currentThread().getName(),
                                             Thread.currentThread().getId(),
                                             _reader.getName(), _reader.getQueueName());
                _log.info(msg);
            }
            return true;
        }
        
        // Not interrupted.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* shutdownReader:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Place a shutdown message on this tenant's recovery queue.  This is a
     * best effort try.
     */
    private void shutdownReader()
    {
        // Create a shutdown message.
        RecoverShutdownMsg shutdownMsg = new RecoverShutdownMsg(this.getClass().getSimpleName());
        
        // Add the parameter values to the command.
        shutdownMsg.setQueueName(_reader.getQueueName());
        shutdownMsg.setForce(true);
        
        // Post the message to the tenant recovery queue.
        JobQueueManager qm = JobQueueManager.getInstance();
        try {qm.postRecoveryQueue(shutdownMsg);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QUEUE_POST_ERROR", "RecoverShutdownMsg", e.getMessage());
            _log.error(msg, e);
        }

    }
}

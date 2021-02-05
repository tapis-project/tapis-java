package edu.utexas.tacc.tapis.jobs.reader;

import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobCancelRecoverMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryManager;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class implements a short-lived thread that takes a JobCancelRecoverMsg
 * object on construction and processes it when the thread runs.  The thread
 * terminates once cancellation completes.
 * 
 * While this thread exists, it runs concurrently to the main recovery manager 
 * thread.  The RecoveryManager class handles synchronization.
 * 
 * @author rcardone
 */
public class RecoveryCancelThread 
 extends Thread 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoveryCancelThread.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private final RecoveryReader      _reader;
    private final JobCancelRecoverMsg _cancelMsg;
    
    // We anchor references to all cancellation threads in this set so that
    // the main recovery class can fire-and-forget about cancellation threads.
    // This gimmick depends on the tradition of JVMs not unloading classes
    // once they are loaded.
    private static final Hashtable<RecoveryCancelThread, String> _anchorSet = new Hashtable<>();

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public RecoveryCancelThread(ThreadGroup threadGroup, String threadName,
                                RecoveryReader reader, JobCancelRecoverMsg cancelMsg)
    {
        // Save inputs, none of which can be null.
        super(threadGroup, threadName);
        _reader = reader;
        _cancelMsg = cancelMsg;
        
        // Add ourselves to the anchor set so we don't get garbage collected.
        _anchorSet.put(this, "");
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
        // No exception escapes...
        try {
            // Tracing.
            if (_log.isInfoEnabled()) 
                _log.info(MsgUtils.getMsg("JOBS_RECOVERY_CANCEL_THREAD_STARTING", 
                                          Thread.currentThread().getName(),
                                          Thread.currentThread().getId(),
                                          _reader.getName(), _cancelMsg.jobUuid));
        
            // Create the recovery manager if it doesn't already exist.
            RecoveryManager mgr = null;
            try {mgr = RecoveryManager.getInstance(_reader);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("JOBS_RECOVERY_INIT_MGR_ERROR", 
                                                 _reader.getName(), _reader.getQueueName());
                    _log.error(msg, e);
                    return;
                }
            
            // Issue the cancel command on the job.  True is returned  
            // only if a job was in recovery and is now cancelled. This
            // thread executes with exclusive database access.
            boolean cancelled;
            synchronized (_reader.getDBLock()) {
                cancelled = mgr.cancelRecovery(_cancelMsg);
            }
        
            // Say goodbye.
            if (_log.isInfoEnabled()) 
                _log.info(MsgUtils.getMsg("JOBS_RECOVERY_CANCEL_THREAD_STOPPING", 
                                          Thread.currentThread().getName(),
                                          Thread.currentThread().getId(),
                                          _reader.getName(), _cancelMsg.jobUuid, 
                                          cancelled));
        }
        catch (Exception e) {
            // This could be an interrupt exception or something more ominous.
            // We swallow the exception and let the thread terminate.
            _log.warn(MsgUtils.getMsg("JOBS_RECOVERY_CANCEL_THREAD_EXCEPTION", 
                      Thread.currentThread().getName(), Thread.currentThread().getId(),
                      _reader.getName(), _cancelMsg.jobUuid), e.getMessage(), e);
        }
        finally {
            // Always remove ourselves from the static anchor set 
            // to make this thread available for garbage collection.
            _anchorSet.remove(this);
        }
    }
}

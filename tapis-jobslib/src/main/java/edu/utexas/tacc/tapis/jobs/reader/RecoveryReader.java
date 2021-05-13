package edu.utexas.tacc.tapis.jobs.reader;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParseException;
import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.dao.JobRecoveryDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager.ExchangeUse;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobCancelRecoverMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.RecoverMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.RecoverShutdownMsg;
import edu.utexas.tacc.tapis.jobs.utils.Throttle;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** This class drives all job recovery by servicing messages placed on the 
 * recovery queue.  The main thread is the designated RabbitMQ queue reader 
 * and another thread, the _recoveryReaderThread, actually invokes recovery
 * actions.  Both of these threads are long-lived.  Recovery cancellation
 * messages are serviced by short-lived threads that are spawned to process
 * a single cancellation and then terminate.
 * 
 * Instances of this class try to keep running when experiencing most errors.
 * If, however, the main remote queue reading thread cannot access the database
 * or the queue broker a runtime exception is thrown and the program terminates.
 * 
 * A future goal is to make this class resilient against database and queue
 * broker failures. 
 * 
 * @author rcardone
 */
public final class RecoveryReader
 extends AbstractQueueReader
 implements Thread.UncaughtExceptionHandler
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoveryReader.class);
    
    // Recovery manager thread definitions.
    private static final String RECOVERY_THREADGROUP_SUFFIX = "-recoveryTG";
    private static final String RECOVERY_MANAGER_THREAD_SUFFIX = "-mgr";
    private static final String SHUTDOWN_THREAD_SUFFIX = "-shutdown";
    private static final String RECOVERY_CANCEL_THREAD_SUFFIX = "-cancel-";
    
    // Thread throttling settings.
    private static final int THREAD_RESTART_SECONDS = 300;
    private static final int THREAD_RESTART_LIMIT = 50;
    
    // The time to wait before actually shutting down.
    private static final long SHUTDOWN_DELAY_MILLIS = 2000;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The tenant's recovery queue name.
    private final String           _queueName;
    
    // Name of the exchange used by this queue.
    private final String           _exchangeName;
    
    // Database access.
    private final JobsDao          _jobsDao;
    private final JobRecoveryDao   _recoveryDao;   
    
    // The thread restart throttle limits the number 
    // of threads started within a time window.
    private final Throttle         _threadRestartThrottle = initThreadRestartThrottle();
    
    // Recovery thread fields.
    private final ThreadGroup     _recoveryThreadGroup;
    private RecoveryReaderThread  _recoveryReaderThread;
    
    // The queue the reader thread writes and the recover thread reads.
    private final LinkedBlockingQueue<JobRecovery> _recoverQueue;
    
    // A monotonically increasing counter of created cancellation threads.
    // Also used as a suffix on cancellation thread names.
    private long                  _cancelThreadCounter;
    
    // This object's lock is used to serialize access to the
    // database among all reader threads. The holder of this
    // lock has exclusive access to the database.
    private final Object          _dbLock = new Object();
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public RecoveryReader(QueueReaderParameters parms) 
     throws TapisRuntimeException, TapisException
    {
        // Assign superclass field.
        super(parms);
        
        // Force use of the default binding key.
        _parms.bindingKey = JobQueueManagerNames.DEFAULT_BINDING_KEY;
        
        // Save the topic name in a field.
        _queueName = JobQueueManagerNames.getRecoveryQueueName();
        
        // Save the exchange name;
        _exchangeName = JobQueueManagerNames.getRecoveryExchangeName();
        
        // Allow access to our tables.
        _jobsDao = new JobsDao();
        _recoveryDao = new JobRecoveryDao();
        
        // Thread group of spawned manager thread.
        _recoveryThreadGroup = new ThreadGroup(_parms.name + RECOVERY_THREADGROUP_SUFFIX);
        
        // Create the queue the recover thread blocks on.
        _recoverQueue = new LinkedBlockingQueue<>();
        
        // Print configuration.
        _log.info(getStartUpInfo(_queueName, _exchangeName));
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) 
     throws TapisRuntimeException, TapisException 
    {
        // Parse the command line parameters.
        QueueReaderParameters parms = null;
        try {parms = new QueueReaderParameters(args);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_START_ERROR", e.getMessage());
            _log.error(msg, e);
            throw e;
          }
        
        // Start the worker.
        RecoveryReader reader = new RecoveryReader(parms);
        reader.start();
    }

    /* ---------------------------------------------------------------------- */
    /* start:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Initialize the process and its threads. */
    public void start()
      throws JobException
    {
      // Announce our arrival.
      if (_log.isInfoEnabled()) 
          _log.info(MsgUtils.getMsg("JOBS_READER_STARTED", _parms.name, 
                                    _queueName, getBindingKey()));
      
      // Initialize the recovery framework.
      initRecoveryReaderThread();
      
      // Start reading the queue.
      readQueue();
      
      // Announce our termination.
      if (_log.isInfoEnabled()) 
          _log.info(MsgUtils.getMsg("JOBS_READER_STOPPED", _parms.name, 
                                    _queueName, getBindingKey()));
    }
    
    /* ********************************************************************** */
    /*                            Protected Methods                           */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDBLock:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Return the lock object to child threads that want to access the database.
     * The basic protocol is that only 1 thread at a time can access the 
     * database and, in general, interleaving thread access to the database
     * while threads are performing one logical task is not a good idea.  
     * Therefore, this lock is used at coarse granularity to let each thread
     * complete its task with sole access to the database.
     * 
     * This coarse granularity approach certainly limits parallelism, which in
     * a way might have the side effect of limiting thrashing where a job
     * repeatedly becomes blocked and then unblocked.
     * 
     * @return
     */
    protected Object getDBLock() {return _dbLock;}
    
    /* ---------------------------------------------------------------------- */
    /* process:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Process a delivered message.
     * 
     * @param delivery the incoming message and its metadata
     * @return true if the message was successfully processed, false if the 
     *          message should be rejected and discarded without redelivery
     */
    @Override
    protected boolean process(DeliveryResponse delivery)
    {
      // Tracing
      if (_log.isDebugEnabled()) { 
          String msg = JobQueueManager.getInstance().dumpMessageInfo(
            delivery.consumerTag, delivery.envelope, delivery.properties, delivery.body);
          _log.debug(msg);
      }
      
      // The body should always be a UTF-8 json string.
      String body;
      try {body = new String(delivery.body, "UTF-8");}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("ALOE_BYTE_ARRAY_DECODE", new String(Hex.encodeHex(delivery.body)));
              _log.error(msg);
              return false;
          }
      
      // Decode the input.
      RecoverMsg recoverMsg = null;
      try {recoverMsg = TapisGsonUtils.getGson(true).fromJson(body, RecoverMsg.class);}
          catch (Exception e) {
              if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
              String msg = MsgUtils.getMsg("ALOE_JSON_PARSE_ERROR", getName(), body, e.getMessage());
              _log.error(msg, e);
              return false;
          }
      
      // Make sure we got some message type.
      if (recoverMsg.getMsgType() == null) {
          String msg = MsgUtils.getMsg("JOBS_WORKER_INVALD_MSG_TYPE", "null", getName());
          _log.error(msg);
          return false;
      }
      
      // Determine the precise command type, populate an object of that type
      // and then call the command-specific processor.
      boolean ack = true;
      try {
          // Any work done by this thread is done with exclusive access to the database.
          synchronized (_dbLock) 
          {
              switch (recoverMsg.getMsgType()) {
                  case RECOVER:  
                      ack = processMsg(TapisGsonUtils.getGson(true).fromJson(body, JobRecoverMsg.class));
                      break;
                  case CANCEL_RECOVER: 
                      ack = processMsg(TapisGsonUtils.getGson(true).fromJson(body, JobCancelRecoverMsg.class));
                      break;
                  case RECOVER_SHUTDOWN: 
                      ack = processMsg(TapisGsonUtils.getGson(true).fromJson(body, RecoverShutdownMsg.class));
                      break;
                  default:
                      ack = processMsg(recoverMsg); // This should not happen.
              }
          }
      }
      catch (TapisRuntimeException e) {
          // A fatal error has occurred, we're shutting down 
          // this program by exiting the main thread.
          String msg = MsgUtils.getMsg("JOBS_RECOVERY_RUNTIME_ERROR", _parms.name, e.getMessage());
          _log.error(msg, e);
          throw e;
      }
      catch (JsonParseException e) {
          if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
          String msg = MsgUtils.getMsg("TAPIS_JSON_PARSE_ERROR", getName(), body, e.getMessage());
          _log.error(msg, e);
          ack = false;
      }
      catch (Exception e) {
          if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
          String msg = MsgUtils.getMsg("JOBS_WORKER_MSG_PROCESSING_ERROR", getName(), 
                                       body, e.getMessage());
          _log.error(msg, e);
          ack = false;
      }

      return ack;
    }

    /* ---------------------------------------------------------------------- */
    /* pollRecoveryQueue:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Retrieve the next element in the queue waiting at most waitMillis 
     * milliseconds.  A wait time of zero or less will return immediately.
     * 
     * @param waitMillis maximum milliseconds to wait when queue is empty 
     * @return the next message or null if a timeout occurred
     * @throws InterruptedException if interrupted while waiting
     */
    protected JobRecovery pollRecoveryQueue(long waitMillis) 
     throws InterruptedException
    {
        return _recoverQueue.poll(waitMillis, TimeUnit.MILLISECONDS);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getName:                                                               */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getName() {return _parms.name;}

    /* ---------------------------------------------------------------------- */
    /* getExchangeType:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    protected BuiltinExchangeType getExchangeType() {return BuiltinExchangeType.FANOUT;}
    
    /* ---------------------------------------------------------------------- */
    /* getExchangeName:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getExchangeName() {return _exchangeName;}
    
    /* ---------------------------------------------------------------------------- */
    /* getExchangeUse:                                                              */
    /* ---------------------------------------------------------------------------- */
    @Override
    protected ExchangeUse getExchangeUse() {return ExchangeUse.OTHER;}
    
    /* ---------------------------------------------------------------------- */
    /* getQueueName:                                                          */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getQueueName() {return _queueName;}

    /* ---------------------------------------------------------------------- */
    /* getBindingKey:                                                         */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getBindingKey() {return _parms.bindingKey;}

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* createRecoveryManagerThreadName:                                       */
    /* ---------------------------------------------------------------------- */
    private String createRecoveryManagerThreadName() 
    {
      return _parms.name + RECOVERY_MANAGER_THREAD_SUFFIX;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createShutdownThreadName:                                              */
    /* ---------------------------------------------------------------------- */
    private String createShutdownThreadName() 
    {
      return _parms.name + SHUTDOWN_THREAD_SUFFIX;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createRecoveryCancelThreadName:                                        */
    /* ---------------------------------------------------------------------- */
    private String createRecoveryCancelThreadName() 
    {
      return _parms.name + RECOVERY_CANCEL_THREAD_SUFFIX + ++_cancelThreadCounter;
    }
    
    /* ---------------------------------------------------------------------- */
    /* initRecoveryReaderThread:                                              */
    /* ---------------------------------------------------------------------- */
    private void initRecoveryReaderThread()
    {
        // Create the recovery manager thread.
        _recoveryReaderThread = 
           new RecoveryReaderThread(_recoveryThreadGroup, 
                                     createRecoveryManagerThreadName(),
                                     this);
        
        // Set attributes and start thread.
        _recoveryReaderThread.setDaemon(true);
        _recoveryReaderThread.setUncaughtExceptionHandler(this);
        _recoveryReaderThread.start();
    }
    
    /* ---------------------------------------------------------------------- */
    /* initThreadRestartThrottle:                                             */
    /* ---------------------------------------------------------------------- */
    /** Initialize the thread restart throttle using configuration settings.
     * 
     * @return the configured throttle
     */
    private Throttle initThreadRestartThrottle()
    {
        return new Throttle(THREAD_RESTART_SECONDS, 
                            THREAD_RESTART_LIMIT);
    }
    
    /* ---------------------------------------------------------------------- */
    /* shutdown:                                                              */
    /* ---------------------------------------------------------------------- */
    /** This method allow any package code to signal a shutdown condition to the
     * main thread.
     */
    private void shutdown()
    {
        // Tracing. 
        _log.info(MsgUtils.getMsg("JOBS_RECOVERY_SIGNALING_SHUTDOWN", 
                                 Thread.currentThread().getName()));
      
       // Interrupt the recovery manager thread
       // and any threads it may have started.
       _recoveryThreadGroup.interrupt();
      
       // Close the queue connection.
       JobQueueManager.getInstance().closeConnections(JobQueueManager.DEFAULT_CONN_CLOSE_TIMEOUT_MS);
    }
    
    /* ---------------------------------------------------------------------- */
    /* startShutdownThread:                                                   */
    /* ---------------------------------------------------------------------- */
    private void startShutdownThread(RecoverShutdownMsg message)
    {
        // Create the thread
        Thread shutdownThread = new Thread(_recoveryThreadGroup, createShutdownThreadName()) 
        {
            @Override
            public void run() {
                
                // Always yield to allow any thread that called shutdown() 
                // time to acknowledge receipt of the shutdown command.
                try {Thread.sleep(SHUTDOWN_DELAY_MILLIS);} catch (InterruptedException e) {}
                
                // Call shutdown.
                shutdown();
            }
        };
        
        // Start the thread.
        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }
    
    /* ---------------------------------------------------------------------- */
    /* failJob:                                                               */
    /* ---------------------------------------------------------------------- */
    private void failJob(String jobUuid, String tenantId, String failMsg)
    {
        // Let's try to fail this job since we can't recover it.
        try {_jobsDao.failJob(_parms.name, jobUuid, tenantId, failMsg);}
            catch (Exception e) {
                // Double fault, what a mess.  The job will be left in 
                // a non-terminal state and not on any queue.  It's a zombie.
                String msg = MsgUtils.getMsg("JOBS_WORKER_ZOMBIE_ERROR", 
                                              _parms.name, jobUuid, tenantId);
                _log.error(msg, e);
                try {
                    RuntimeParameters runtime = RuntimeParameters.getInstance();
                    EmailClient client = EmailClientFactory.getClient(runtime);
                    client.send(runtime.getSupportName(),
                        runtime.getSupportEmail(),
                        "Zombie Job Alert " + jobUuid + " is in a zombie state.",
                        msg, HTMLizer.htmlize(msg));
                }
                catch (TapisException ae) {
                    // log msg that we tried to send email notice to CICSupport
                    _log.error(msg+" Failed to send support Email alert. Email client failed with exception. ", ae);
                }
            }
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* processMsg:                                                            */
    /* ---------------------------------------------------------------------- */
    private boolean processMsg(JobRecoverMsg message)
    {
        // We are not interested if the job is not blocked.
        JobStatusType jobStatus = null;
        try {jobStatus = _jobsDao.getStatusByUUID(message.getJobUuid());}
            catch (TapisDBConnectionException e) {
                // For now we consider failing to get a db connection fatal.
                String msg = MsgUtils.getMsg("JOBS_READER_FATAL_DB_ERROR", getName(),
                                             getQueueName(), message.getTenantId(), e.getMessage());
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INPUT_ERROR", e.getMessage());
                _log.error(msg, e);
                
                // Let's try to fail this job since we can't recover it.
                failJob(message.getJobUuid(), message.getTenantId(), msg);                
                return false;
            }
        if (jobStatus != JobStatusType.BLOCKED) {
            String msg = MsgUtils.getMsg("JOBS_RECOVERY_NOT_BLOCKED", message.getJobUuid(),
                                         jobStatus);
            _log.error(msg);
            return false;
        }
        
        // Get the job recovery object including its blocked job.
        JobRecovery jobRecovery = null;
        try {jobRecovery = new JobRecovery(message);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INPUT_ERROR", e.getMessage());
                _log.error(msg, e);
                
                // Let's try to fail this job since we can't recover it.
                failJob(message.getJobUuid(), message.getTenantId(), msg);                
                return false;
            }
        
        // Add the recovery record to the database.  Note that this method
        // updates the id field in jobRecovery with its db-generated value.
        // Both the aloe_job_recovery and aloe_job_blocked tables are updated.
        try {_recoveryDao.addJobRecovery(jobRecovery);}
            catch (TapisDBConnectionException e) {
                // For now we consider failing to get a db connection fatal.
                String msg = MsgUtils.getMsg("JOBS_READER_FATAL_DB_ERROR", getName(),
                                             getQueueName(), message.getTenantId() , e.getMessage());
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INPUT_ERROR", e.getMessage());
                _log.error(msg, e);
                
                // Let's try to fail this job since we can't recover it.
                failJob(message.getJobUuid(), message.getTenantId(), msg);                
                return false;
            }
        
        // If no jobs were actually blocked due to duplicate detection, our work is done.
        // Duplicates are not considered an error condition.
        if (jobRecovery.getBlockedJobs().isEmpty()) return true;
        
        // Firewall against incomplete recovery objects.  We make sure there is NO WAY that
        // a recovery job object can be placed on the internal queue without a valid id.
        if (jobRecovery.getId() <= 0) {
            String msg = MsgUtils.getMsg("JOBS_BAD_RECOVERY_ID", jobRecovery.getTenantId(), 
                                         jobRecovery.getConditionCode().name(), 
                                         jobRecovery.getTesterHash(), jobRecovery.getId());
            _log.error(msg);
            failJob(message.getJobUuid(), message.getTenantId(), msg);                
            return false;
        }
        
        // Send the recover object to the recovery manager. If we die after this
        // call succeeds, the recovery message will be read again from RabbitMQ
        // even though it will may serviced by the recovery manager thread.  That
        // thread must be resilient against duplicate messages.  .
        try {_recoverQueue.put(jobRecovery);} 
            catch (Exception e) {
                // This shouldn't happen since we are using
                // an unbounded linked queue, but just in case.
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_INTERNAL_QUEUE_ERROR", e.getMessage());
                _log.error(msg, e);
                failJob(message.getJobUuid(), message.getTenantId(), msg);                
                return false;
            }
        
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* processMsg:                                                            */
    /* ---------------------------------------------------------------------- */
    private boolean processMsg(JobCancelRecoverMsg message)
    {
        // Create the recovery manager thread.
        RecoveryCancelThread thread = 
           new RecoveryCancelThread(_recoveryThreadGroup, 
                                    createRecoveryCancelThreadName(),
                                    this, message);
        
        // Set attributes and start thread
        // in fire-and-forget mode.
        thread.setDaemon(true);
        thread.start();
        
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* processMsg:                                                            */
    /* ---------------------------------------------------------------------- */
    private boolean processMsg(RecoverShutdownMsg message)
    {
        // Shutdown the recovery manager thread.
        startShutdownThread(message);
        
        // Shutdown this (the main) thread.
        Thread.currentThread().interrupt();
        
        // Acknowledge the queue message.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* processMsg:                                                            */
    /* ---------------------------------------------------------------------- */
    private boolean processMsg(RecoverMsg message)
    {
        // To get here we must of sent a message type that this processor
        // is not expected to handle.
        String msg = MsgUtils.getMsg("JOBS_WORKER_INVALD_MSG_TYPE", message.getMsgType().name(), 
                                     getName());
        _log.error(msg);
        return false;
    }
    
    /* ********************************************************************** */
    /*                            Exception Handler                           */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* uncaughtException:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Deal with an unexpected exception in the recovery manager thread.  The JVM  
     * calls this method when a worker or other registered thread dies.  The intent 
     * is to start another thread of the same type as the dying thread after we log
     * the incident.  
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) 
    {
        // Record the error.
        _log.error(MsgUtils.getMsg("ALOE_THREAD_UNCAUGHT_EXCEPTION", _parms.name, e.toString()));
        e.printStackTrace(); // stderr for emphasis

        // ---- Do nothing if its not a known thread.
        if (!(t instanceof RecoveryReaderThread)) {
            // This shouldn't happen since we determine a compile time which
            // threads we are going to restart using this method.
            String msg = MsgUtils.getMsg("ALOE_THREAD_DIED", t.getName(), t.getId(), 
                                         e.getClass().getSimpleName(), e);
            _log.error(msg);
            return;
        }
        
        // ---- Don't interfere with shutdown.
        // Interrupted exceptions should never be uncaught,
        // but we put this check in for assurance.s
        if (e instanceof InterruptedException) return;
        
        // ---- Start a new recovery manager thread.
        // Get the dead worker.
        RecoveryReaderThread oldWorker = (RecoveryReaderThread) t;
        
        // Are we in a restart storm?
        if (_threadRestartThrottle.record()) {
            // Create the new thread object.  Note that the new thread's
            // recovery queue will be empty but we shouldn't lose any
            // messages since the recovery information is either in
            // the database or still in the RabbitMQ durable queue.
            initRecoveryReaderThread();
            
            // Log more information.
            _log.error(MsgUtils.getMsg("TAPIS_THREAD_RESTART", 
                                       oldWorker.getName(), oldWorker.getId(),
                                       _parms.name, e.getClass().getSimpleName(),
                                       _recoveryReaderThread.getName()), e);
                                           
        }
        else {
            // Too many restarts in the configured time window.
            _log.error(MsgUtils.getMsg("TAPIS_TOO_MANY_RESTARTS_TERMINATION", 
                                       oldWorker.getName(), oldWorker.getId(),
                                       _threadRestartThrottle.getLimit(),
                                       _threadRestartThrottle.getSeconds()));
               
            // Commit harakiri.
            shutdown();
        }
    }
}

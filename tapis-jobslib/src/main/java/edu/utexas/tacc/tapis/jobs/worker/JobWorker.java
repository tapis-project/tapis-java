package edu.utexas.tacc.tapis.jobs.worker;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.messages.event.WkrStatusResp;
import edu.utexas.tacc.tapis.jobs.utils.Throttle;
import edu.utexas.tacc.tapis.jobs.worker.JobQueueProcessor.JobTopicThread;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

public final class JobWorker
 implements Thread.UncaughtExceptionHandler
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobWorker.class);
    
    // Thread throttling settings.
    private static final int JOB_THREAD_RESTART_SECONDS = 300;
    private static final int JOB_THREAD_RESTART_LIMIT = 50;
    private static final int JOB_START_SECONDS = 2;
    private static final int JOB_START_LIMIT = 10;
    
    // Thread group name suffixes.
    private static final String WORKER_THREADGROUP_SUFFIX = "-workerTG";
    private static final String TOPIC_THREADGROUP_SUFFIX  = "-topicTG";
    private static final String JOB_THREADGROUP_SUFFIX    = "-jobTG";
    
    // Thread name components.
    private static final String CMD_TOPIC_THREAD_SUFFIX = "-CmdTopic";
    static final String         JOB_TOPIC_THREAD_SUFFIX   = "-Job";
    
    // The time to wait before actually shutting down after 
    // the shutdown() method is called.
    private static final long SHUTDOWN_DELAY_MILLIS = 2000;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Command line parameters.
    private final JobWorkerParameters _parms;
    
    // Unique identifier of this worker instance.
    private final TapisUUID      _uuid = new TapisUUID(UUIDType.JOB_WORKER);
       
    // Counter used when naming threads.
    private final AtomicInteger _threadSeqNo = new AtomicInteger(0);

    // The thread restart throttle limits the number 
    // of threads started within a time window.
    private final Throttle      _threadRestartThrottle = initThreadRestartThrottle();
    
    // Limit the number of jobs immediately started within a short time period.
    private final Throttle      _jobStartThrottle = initJobStartThrottle();
    
    // The thread group for all explicitly spawned worker threads in this program.
    private ThreadGroup         _workerThreadGroup;
    
    // The thread group for general topic thread in this program.
    private ThreadGroup         _topicThreadGroup;
    
    // The thread group for job-specific threads spawned by worker threads.
    private ThreadGroup         _jobThreadGroup;
    
    // Shutdown components.  
    private transient boolean   _shuttingDown;      // Flag indicates shutdown
    private final Lock          _shutdownLock;      // The shutdown lock 
    private final Condition     _shutdownCondition; // Lock-associated condition 
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobWorker(JobWorkerParameters parms)
    {
      // Set the logging identifier that is local to this main thread. The
      // logging identifier distinguishes different invocation of this thread.
      MDC.put(TapisConstants.MDC_ID_KEY, TapisUtils.getRandomString());
        
      // Parameters cannot be null.
      if (parms == null) {
        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobWorker", "parms");
        _log.error(msg);
        throw new IllegalArgumentException(msg);
      }
      _parms = parms;
      
      // Initialize the shutdown data structures.
      _shutdownLock = new ReentrantLock();
      _shutdownCondition = _shutdownLock.newCondition();
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) 
      throws JobException
    {
        // Parse the command line parameters.
        JobWorkerParameters parms = null;
        try {parms = new JobWorkerParameters(args);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_START_ERROR", e.getMessage());
            _log.error(msg, e);
            throw e;
          }
        
        // Start the worker.  Log errors here.
        JobWorker worker = new JobWorker(parms);
        try {worker.start();}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                throw e;
            }
    }

    /* ---------------------------------------------------------------------- */
    /* start:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Initialize the process and its threads. */
    public void start()
      throws JobException
    {
      // Force runtime parameters to be initialized early.
      RuntimeParameters.getInstance();
        
      // Announce our arrival.
      if (_log.isInfoEnabled()) _log.info(getStartUpInfo());
      
      // Initalize the tenants, service context, queue broker and db.
      // Exceptions can be thrown from here.
      initWorkerEnv();
      
      // Create all threads groups used by this worker.
      createThreadGroups();
      
      // Start the general topic thread.
      startCmdTopicThread();
      
      // Start the worker threads.
      startJobQueueThreads();
      
      // Wait for the last thread to complete.
      waitForShutdown();
      
      // Clean up.
      cleanUp();
      
      // Announce our demise.
      if (_log.isInfoEnabled()) 
        _log.info(MsgUtils.getMsg("JOBS_WORKER_TERMINATING", _parms.name, _uuid.toString()));
    }
    
    /* ********************************************************************** */
    /*                             Package Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* shutdown:                                                              */
    /* ---------------------------------------------------------------------- */
    /** This method allow any package code to signal a shutdown condition to the
     * main thread.
     */
    void shutdown()
    {
      // Acquire the shutdown lock, set the shutdown flag and signal all
      // threads (i.e., the main thread) that the shutdown condition has changed.
      _shutdownLock.lock();
      try { 
        _log.info(MsgUtils.getMsg("JOBS_WORKER_SIGNALING_SHUTDOWN", 
                                  Thread.currentThread().getName()));
        _shuttingDown = true;
        _shutdownCondition.signalAll();
      } 
      finally {
        _shutdownLock.unlock();
      }
    }
    
    /* ---------------------------------------------------------------------- */
    /* getParms:                                                              */
    /* ---------------------------------------------------------------------- */
    JobWorkerParameters getParms(){return _parms;}

    /* ---------------------------------------------------------------------- */
    /* getUUID:                                                               */
    /* ---------------------------------------------------------------------- */
    TapisUUID getUUID() {return _uuid;}
    
    /* ---------------------------------------------------------------------- */
    /* getJobThreadGroup:                                                     */
    /* ---------------------------------------------------------------------- */
    ThreadGroup getJobThreadGroup() {return _jobThreadGroup;}
    
    /* ---------------------------------------------------------------------- */
    /* getWorkerStatusResp:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Create a response to a worker status message to be queue on the tenant
     * event queue.
     * 
     * @param correlationId the request message correlation id
     * @param senderId the request message sender id
     * @return the populated status response message
     */
    WkrStatusResp getWorkerStatusResp(String correlationId, String senderId)
    {
        // Initialize the response message with request information.
        WkrStatusResp resp = new WkrStatusResp();
        resp.correlationId = correlationId;
        resp.senderId = senderId;
        
        // Fill in worker state.
        resp.workerUuid  = _uuid.toString();
        resp.workerParms = _parms;
        
        resp.threadSeqNo = _threadSeqNo.get();
        resp.throttleLimit = _threadRestartThrottle.getLimit();
        resp.throttleWindowSeconds = _threadRestartThrottle.getSeconds();
        resp.throttleQueueLength = _threadRestartThrottle.getQueueLength();
        
        resp.workerThreadGroupName       = _workerThreadGroup.getName();
        resp.workerThreadGroupNumThreads = _workerThreadGroup.activeCount();
        resp.jobThreadGroupName          = _jobThreadGroup.getName();
        resp.jobThreadGroupNumThreads    = _jobThreadGroup.activeCount();
        resp.topicThreadGroupName        = _topicThreadGroup.getName();
        resp.topicThreadGroupNumThreads  = _topicThreadGroup.activeCount();
        
        resp.shuttingDown = _shuttingDown;
        
        return resp;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJobStartThrottle:                                                   */
    /* ---------------------------------------------------------------------- */
    Throttle getJobStartThrottle() {return _jobStartThrottle;}
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initWorkerEnv:                                                         */
    /* ---------------------------------------------------------------------- */
    private void initWorkerEnv()
     throws JobException
    {
        // Already initiailized, but assigned for convenience.
        var parms = RuntimeParameters.getInstance();
        
        // Enable more detailed SSH logging if the node name is not null.
        SSHConnection.setLocalNodeName(parms.getLocalNodeName());
        
        // Force runtime initialization of the tenant manager.  This creates the
        // singleton instance of the TenantManager that can then be accessed by
        // all subsequent application code--including filters--without reference
        // to the tenant service base url parameter.
        Map<String,Tenant> tenantMap = null;
        try {
            // The base url of the tenants service is a required input parameter.
            // We actually retrieve the tenant list from the tenant service now
            // to fail fast if we can't access the list.
            String url = parms.getTenantBaseUrl();
            tenantMap = TenantManager.getInstance(url).getTenants();
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "TenantManager", e.getMessage());
            throw new JobException(msg, e);
        }
        if (!tenantMap.isEmpty()) {
            String msg = ("\n--- " + tenantMap.size() + " tenants retrieved:\n");
            for (String tenant : tenantMap.keySet()) msg += "  " + tenant + "\n";
            _log.info(msg);
        } else {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "TenantManager", "Empty tenant map.");
            throw new JobException(msg);
        }
        
        // ----- Service JWT Initialization
        ServiceContext serviceCxt = ServiceContext.getInstance();
        try {
                 serviceCxt.initServiceJWT(parms.getSiteId(), TapisConstants.SERVICE_NAME_JOBS, 
                                           parms.getServicePassword());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "ServiceContext", e.getMessage());
            throw new JobException(msg, e);
        }
        // Print site info.
        {
            var targetSites = serviceCxt.getServiceJWT().getTargetSites();
            int targetSiteCnt = targetSites != null ? targetSites.size() : 0;
            String msg = "\n--- " + targetSiteCnt + " target sites retrieved:\n";
            if (targetSites != null) {
                for (String site : targetSites) msg += "  " + site + "\n";
            }
            _log.info(msg);
        }
        
        // ----- Database Initialization
        try {JobsImpl.getInstance().ensureDefaultQueueIsDefined();}
         catch (Exception e) {
             String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "JobQueuesDao", e.getMessage());
             throw new JobException(msg, e);
         }
        
        // ------ Queue Initialization 
        // Establish our connection to the queue broker.
        // and initialize queues and topics.  There is 
        // some redundancy here since each front-end and
        // each worker initialize all queue artifacts.  
        // Not a problem, but there's room for improvement.
        try {JobQueueManager.getInstance(JobQueueManager.initParmsFromRuntime());}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "JobQueueManager", e.getMessage());
                throw new JobException(msg, e);
        }   
        
        // We're done.
        _log.info(MsgUtils.getMsg("JOBS_WORKER_INIT_COMPLETE"));
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
        return new Throttle(JOB_THREAD_RESTART_SECONDS, 
                            JOB_THREAD_RESTART_LIMIT);
    }
    
    /* ---------------------------------------------------------------------- */
    /* initJobStartThrottle:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Initialize the number of jobs that we start executing in a short time
     * interval.  This throttle allows us to pace job SSH calls when a large
     * number of jobs are submitted at once.  Delayed jobs have have a good
     * chance of reading accurate quota statuses from the database. 
     * 
     * @return the configured throttle
     */
    private Throttle initJobStartThrottle()
    {
        return new Throttle(JOB_START_SECONDS, JOB_START_LIMIT);
    }
    
    /* ---------------------------------------------------------------------- */
    /* startCmdTopicThread:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Start the single topic thread that subscribes to commands for all tenant
     * workers and for this specific tenant worker. 
     */
    private void startCmdTopicThread()
    {
      CmdTopicThread topicWorker = new CmdTopicThread();
      
      // Set attributes.
      topicWorker.setDaemon(true);
      topicWorker.setUncaughtExceptionHandler(this);
      topicWorker.start();
    }
    
    /* ---------------------------------------------------------------------- */
    /* startJobQueueThreads:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Start the configured number of worker threads. */
    private void startJobQueueThreads()
    {
      // Create and start the required number of worker threads.
      for (int i = 0; i < _parms.numWorkers; i++) {
        // Create the new thread.
        JobQueueThread worker = new JobQueueThread();
        
        // Set attributes.
        worker.setDaemon(true);
        worker.setUncaughtExceptionHandler(this);
        worker.start();
      }
    }
    
    /* ---------------------------------------------------------------------- */
    /* createThreadGroups:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Create the 3 thread groups used by this worker process. */
    private void createThreadGroups()
    {
      // The group for all worker threads (threads that wait for new job submissions).
      _workerThreadGroup = new ThreadGroup(_parms.name + WORKER_THREADGROUP_SUFFIX);
      
      // The child group for all job-specific threads (threads spawned by worker threads).
      _jobThreadGroup = new ThreadGroup(_workerThreadGroup, _parms.name + JOB_THREADGROUP_SUFFIX);
      
      // The group for all topic threads (threads that wait for commands).
      _topicThreadGroup = new ThreadGroup(_parms.name + TOPIC_THREADGROUP_SUFFIX);
    }
    
    /* ---------------------------------------------------------------------- */
    /* createWorkerThreadName:                                                */
    /* ---------------------------------------------------------------------- */
    private String createWorkerThreadName() {
      return _parms.name + "-" + _threadSeqNo.incrementAndGet();
    }
    
    /* ---------------------------------------------------------------------- */
    /* createCmdThreadName:                                                   */
    /* ---------------------------------------------------------------------- */
    private String createCmdThreadName() {
      return _parms.name + CMD_TOPIC_THREAD_SUFFIX;
    }
    
    /* ---------------------------------------------------------------------- */
    /* waitForShutdown:                                                       */
    /* ---------------------------------------------------------------------- */
    /** The main thread of this program waits on a condition variable after
     * starting all threads.  All threads that are spawned are daemon threads so
     * as soon as the main thread exits so will the JVM.  The main thread will
     * also shutdown if it is interrupted. 
     */
    private void waitForShutdown()
    {
        // Acquire the shutdown lock and then wait on the condition variable.
        // Other threads use the shutdown() method to signal that it's time
        // to shutdown.  
        _shutdownLock.lock();
        try {
            // Wait for the signal to check whether to shutdown.
            while (!_shuttingDown) {
                _shutdownCondition.await();
            }
            
            // We're going down.
            _log.info(MsgUtils.getMsg("JOBS_WORKER_SHUTDOWN_SIGNAL", _parms.name));
        } 
        catch (InterruptedException e) {
            // We're going down.
            _log.info(MsgUtils.getMsg("JOBS_WORKER_SHUTDOWN_INTERRUPT", _parms.name));
        }
        finally {
            // Free ownership of the lock.
            _shutdownLock.unlock();
        }
        
        // Always yield to allow any thread that called shutdown() 
        // time to acknowledge receipt of the shutdown command.
        try {Thread.sleep(SHUTDOWN_DELAY_MILLIS);} catch (InterruptedException e) {}
    }
    
    /* ---------------------------------------------------------------------- */
    /* cleanUp:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Perform any clean up before this worker exits. */
    private void cleanUp()
    {
        // Get the queue manager.
        JobQueueManager qm = JobQueueManager.getInstance();
        
        // Try to clean up the worker-specific queue binding before we exit.
        qm.unbindWorkerSpecificCmdTopic(_parms.name, _uuid.toString());
        
        // Shutdown the connections to the queue broker.
        qm.closeConnections(JobQueueManager.DEFAULT_CONN_CLOSE_TIMEOUT_MS);
        
        // Shutdown the database connections.
        TapisDataSource.close();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getStartUpInfo:                                                        */
    /* ---------------------------------------------------------------------- */
    private String getStartUpInfo()
    {
      // Get the already initialized runtime configuration.
      RuntimeParameters runParms = RuntimeParameters.getInstance();
        
      // Dump the parms.
      StringBuilder buf = new StringBuilder(2500); // capacity to avoid resizing
      buf.append("\n------- Starting JobWorker ");
      buf.append(_parms.name);
      buf.append(" -------");
      buf.append("\nUnique ID: ");
      buf.append(_uuid.toString());
      buf.append("\nQueue Name: ");
      buf.append(_parms.queueName);
      buf.append("\nWorker Threads: ");
      buf.append(_parms.numWorkers);
      buf.append("\nAllow Test Parameters: ");
      buf.append(_parms.allowTestParms);
      buf.append("\nTest User: ");
      buf.append(_parms.testUser);
      
      // Dump the runtime configuration.
      runParms.getRuntimeInfo(buf);
      buf.append("\n---------------------------------------------------\n");
      
      return buf.toString();
    }
    
    /* ********************************************************************** */
    /*                            Exception Handler                           */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* uncaughtException:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Recover worker threads from an unexpected exceptions.  The JVM calls 
     * this method when a worker or other registered thread dies.  The intent is 
     * to start another thread of the same type as the dying thread after we log
     * the incident.  
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) 
    {
        // Record the error.
        _log.error(MsgUtils.getMsg("TAPIS_THREAD_UNCAUGHT_EXCEPTION", _parms.name, e.toString()));
        e.printStackTrace(); // stderr for emphasis
        
        // ---- Do nothing if its not a known thread.
        if (!(t instanceof JobWorkerThread)) {
            // This shouldn't happen since we determine a compile time which
            // threads we are going to restart using this method.
            String msg = MsgUtils.getMsg("TAPIS_THREAD_DIED", t.getName(), t.getId(), 
                                         e.getClass().getSimpleName(), e);
            _log.error(msg);
            return;
        }
        
        // ---- Don't interfere with shutdown.
        if (_shuttingDown) return;
        
        // ---- Start a new worker thread.
        // Get the dead worker.
        JobWorkerThread oldWorker = (JobWorkerThread) t;
        
        // Are we in a restart storm?
        if (_threadRestartThrottle.record()) {
            // Create the new thread object.
            JobWorkerThread newWorker = null;
            if (oldWorker instanceof JobQueueThread)
                newWorker = new JobQueueThread();
            else if (oldWorker instanceof CmdTopicThread)
                newWorker = new CmdTopicThread();
            else if (oldWorker instanceof JobTopicThread) {
                JobTopicThread jobThread = (JobTopicThread) oldWorker;
                newWorker = jobThread.getEnclosing().new JobTopicThread(jobThread.getName(), jobThread.getJob());
            }
            else {
              // We have an unknown subclass of JobWorkerThread!
              // Log the information and return.
              _log.error(MsgUtils.getMsg("TAPIS_THREAD_UNKNOWN_RESTART_TYPE", 
                                         oldWorker.getName(), oldWorker.getId(),
                                         _parms.name, _uuid.toString(), 
                                         e.getClass().getSimpleName(),
                                         oldWorker.getClass().getSimpleName()), e);
              return;
            }
              
            // Set attributes.
            newWorker.setDaemon(true);
            newWorker.setUncaughtExceptionHandler(this);
                
            // Log more information.
            _log.error(MsgUtils.getMsg("TAPIS_THREAD_RESTART", 
                                       oldWorker.getName(), oldWorker.getId(),
                                       _parms.name, _uuid.toString(), 
                                       e.getClass().getSimpleName(),
                                       newWorker.getName()), e);
                                           
            // Let it rip.
            newWorker.start();
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
    
    /* ********************************************************************** */
    /*                          JobQueueThread Class                          */
    /* ********************************************************************** */
    /** This class reads the configured tenant queue and processes jobs as they arrive. */
    private final class JobQueueThread extends JobWorkerThread
    {
      // Constructor
      private JobQueueThread() 
      {
        super(_workerThreadGroup, createWorkerThreadName(), JobWorker.this, 
              _parms.queueName, new JobQueueProcessor(JobWorker.this));
      }
    }
    
    /* ********************************************************************** */
    /*                           CmdTopicThread Class                         */
    /* ********************************************************************** */
    /** This class reads the configured tenant topic and processes commands as 
     * they arrive. 
     */
    private final class CmdTopicThread extends JobWorkerThread
    {
      // Constructor
      private CmdTopicThread() 
      {
        super(_topicThreadGroup, createCmdThreadName(), JobWorker.this, 
              JobQueueManagerNames.getCmdTopicName(_parms.name), 
              new CmdTopicProcessor(JobWorker.this));
      }
      
      // Unbind the job-specific key.
      @Override
      protected void cleanUp(AbstractProcessor processor, JobWorker jobWorker, String qname) 
      {
          // Try to remove this worker's binding key from the tenant command exchange.
          JobQueueManager.getInstance().unbindWorkerSpecificCmdTopic(_parms.name, _uuid.toString());  
      }
    }
}

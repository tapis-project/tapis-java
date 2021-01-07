package edu.utexas.tacc.tapis.jobs.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.model.JobQueue;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.event.EventMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.RecoverMsg;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public final class QueueManager 
  extends QueueManagerNames
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(QueueManager.class);
    
  // Binding key to use with fanout exchanges.
  public static final String DEFAULT_BINDING_KEY = "";
  
  // Default timeout in milliseconds to close a connection.
  public static final int DEFAULT_CONN_CLOSE_TIMEOUT_MS = 1000;
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Singleton instance of this class.
  private static QueueManager  _instance;
  
  // Fields that get initialized once and tend not to change.
  private ConnectionFactory    _factory;
  private Connection           _outConnection;
  private Connection           _inConnection;

  /* ********************************************************************** */
  /*                             Constructors                               */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  /** This constructor creates all job worker queues and exchanges by querying 
   * the job_queues table.  It also creates each active tenant's command and 
   * event topics and their exchanges by accessing data in the tenants table.
   * Without the ability to connect to the database, not much is going to get
   * done even though we don't throw exceptions.
 * @throws TapisException 
   */
  private QueueManager() throws TapisRuntimeException
  {
      // ************** TODO: implement
      
      
      // Get all active tenant ids.
//      List<String> tenantIds;
//      try {
//          TenantDao tenantDao = new TenantDao(JobQueueDao.getDataSource());
//          tenantIds = tenantDao.getTenantIds();
//      }
//      catch (Exception e) {
//          String msg = MsgUtils.getMsg("TAPIS_TENANTS_QUERY_ERROR");
//          throw new TapisException(msg, e);
//      }
      
      // Try to create the tenant event topic but allow 
      // instance creation even in the face of failures.
//      try {createStandardQueues(tenantIds);}
//      catch (Exception e) {
//        String msg = MsgUtils.getMsg("JOBS_QMGR_INIT_ERROR", "queue");
//        throw new TapisException(msg, e);
//      }
      
      // Try to create all tenant queues but allow 
      // instance creation even in the face of failures. 
//      try {createTenantDefinedQueues();}
//      catch (Exception e) {
//          String msg = MsgUtils.getMsg("JOBS_QMGR_INIT_ERROR", "queue");
//          throw new TapisException(msg, e);
//      }
  }
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getInstance:                                                           */
  /* ---------------------------------------------------------------------- */
  /** Get the singleton instance of this class, creating it if necessary.
   * 
   * @return
 * @throws TapisException 
   */
  public static QueueManager getInstance() throws TapisRuntimeException
  {
    // Create the singleton instance if it's null without
    // setting up a synchronized block in the common case.
    if (_instance == null) {
      synchronized (QueueManager.class) {
        if (_instance == null) _instance = new QueueManager();
      }
    }
    return _instance;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getNewOutChannel:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Return a new outbound channel on the existing queuing system connection.
   * 
   * @return the new channel
   * @throws JobSchedulerException on error
   */
  public Channel getNewOutChannel()
    throws JobQueueException
  {
      // Create a new channel in this phase's connection.
      Channel channel = null;
      try {channel = getOutConnection().createChannel();} 
       catch (IOException e) {
           String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CREATE_ERROR", 
                                        getOutConnectionName(), e.getMessage());
           _log.error(msg, e);
           throw new JobQueueException(msg, e);
       }
      
      // Tracing.
      if (_log.isInfoEnabled()) 
          _log.info("Created channel number " + channel.getChannelNumber() + 
                    " on " + getOutConnectionName() + ".");
       
      return channel;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getNewInChannel:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Return a new inbound channel on the existing queuing system connection.
   * 
   * @return the new channel
   * @throws JobSchedulerException on error
   */
  public Channel getNewInChannel()
    throws JobQueueException
  {
      // Create a new channel in this phase's connection.
      Channel channel = null;
      try {channel = getInConnection().createChannel();} 
       catch (IOException e) {
           String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CREATE_ERROR", 
                                        getOutConnectionName(), e.getMessage());
           _log.error(msg, e);
           throw new JobQueueException(msg, e);
       }
      
      // Tracing.
      if (_log.isInfoEnabled()) 
          _log.info("Created channel number " + channel.getChannelNumber() + 
                    " on " + getInConnectionName() + ".");
       
      return channel;
  }
  
  /* ---------------------------------------------------------------------- */
  /* doRefreshQueueInfo:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Force all schedulers to refresh their queue caches.  This action allows
   * any changes to queue definitions in the job_queues table to be loaded into
   * memory.  For example, if the priority of a queue has changed in the 
   * database, its new priority will be respected after this method executes.
   * 
   * How This Works
   * --------------
   * Users can execute code in JobQueueDao to change the persistent information
   * about queues, such as the priority of a queue.  These changes, however, 
   * are not reflected in the running system until this method is executed and
   * the cached _tenantQueues map is replaced.   
   * 
   * Being able to switch out the old _tenantQueues map for a new one in a 
   * multithreaded environment depends on guarantees implemented in the JVM.  
   * Specifically, the Java Language Specification requires that reference assignments 
   * be atomically performed.  That is, when updating a reference to an object, all 
   * threads see either the old address or the new address, but never a mixture of 
   * the two.  Here's the quote from Java 7's Java Language Specification, section 17.7:
   * 
   *    Writes to and reads of references are always atomic, regardless  
   *    of whether they are implemented as 32-bit or 64-bit values.
   * 
   * Our usage of the _tenantQueues mapping abides by these rules:
   * 
   *  1) The field is only written on initialization and in this method.
   *  2) Once created, the mapping is never modified.  This method updates
   *     the mapping by overwriting the field with a completely new reference.
   *  3) All reads of the field are single, independent accesses that do
   *     not intermix data with other reads of the field.
   * 
   * These three access patterns, taken together with Java's atomicity guarantee,
   * means that the _tenantQueues reference can be overwritten at any time
   * without synchronization between reader and writer threads.
   * 
   * @return true to acknowledge the message, false to reject it
   */
  public boolean doRefreshQueueInfo(//JobCommand jobCommand, 
                                    Envelope envelope, 
                                    BasicProperties properties,
                                    byte[] body)
  {
    // TODO: implement doRefreshQueueInfo()
    return false;
  }
  
  /* ---------------------------------------------------------------------- */
  /* postSubmitQueue:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Write a json message to the named tenant queue.  The queue name is used 
   * as the routing key on the direct exchange.
   * 
   * @param queueName the target queue name
   * @param message a json string
   */
  public void postSubmitQueue(String queueName, String message)
    throws JobException
  {
    // Get the non-null tenantId from the context.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    String tenantId = "xxx";
    
    // Don't allow processing for an uninitialized tenant.
    if (TapisThreadContext.INVALID_ID.equals(tenantId)) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_MISSING_TENANT_ID", queueName);
        _log.error(msg);
        throw new JobException(msg);
    }
    
    // Create a temporary channel.
    Channel channel = null;
    boolean abortChannel = false;
    try {
      // Create a temporary channel.
      try {channel = getNewOutChannel();}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", tenantId);
          _log.error(msg, e);
          throw e;
        }
    
      // Get the tenant exchange name.
      String exchangeName = getTenantSubmitExchangeName(tenantId); 
      
      // Publish the message to the queue.
      try {
        // Write the job to the selected tenant worker queue.
        channel.basicPublish(exchangeName, queueName, QueueManager.PERSISTENT_JSON, 
                             message.getBytes("UTF-8"));
        
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_POST", tenantId, exchangeName, queueName);
            _log.debug(msg);
        }
      }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_PUBLISH_ERROR", exchangeName, 
                                       getOutConnectionName(), channel.getChannelNumber(), 
                                       e.getMessage());
          _log.error(msg, e);
          throw new JobQueueException(msg, e);
        }
    } 
    catch (Exception e) {
      // Affect the way we close the channel and then rethrow exception.
      abortChannel = true;
      throw e;
    }
    finally {
      // Channel clean up
      if (channel != null) {
        try {
          // Close the channel one way or the other.
          if (abortChannel) channel.abort();
            else channel.close();
        } 
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", channel.getChannelNumber(), 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
    }
  }

  /* ---------------------------------------------------------------------- */
  /* postTenantTopic:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Write a json message to the named tenant topic.  The the routing key
   * determines which workers receive the message.
   * 
   * @param exchangeName the target exchange name
   * @param message a json string
   */
  public void postTenantTopic(String exchangeName, String message, String routingKey)
    throws JobException
  {
    // Get the non-null tenantId from the context.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    String tenantId = "xxx";
    
    // Don't allow processing for an uninitialized tenant.
    if (TapisThreadContext.INVALID_ID.equals(tenantId)) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_MISSING_TENANT_ID", exchangeName);
        _log.error(msg);
        throw new JobException(msg);
    }
    
    // Create a temporary channel.
    Channel channel = null;
    boolean abortChannel = false;
    try {
      // Create a temporary channel.
      try {channel = getNewOutChannel();}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", tenantId);
          _log.error(msg, e);
          throw e;
        }
    
      // Publish the message to the queue.
      try {
        // Write the job to the selected tenant worker queue.
        channel.basicPublish(exchangeName, routingKey, QueueManager.PERSISTENT_JSON, 
                             message.getBytes("UTF-8"));
        
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_POST", tenantId, exchangeName, routingKey);
            _log.debug(msg);
        }
      }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_PUBLISH_ERROR", exchangeName,  
                                       getOutConnectionName(), channel.getChannelNumber(), 
                                       e.getMessage());
          _log.error(msg, e);
          throw new JobQueueException(msg, e);
        }
    } 
    catch (Exception e) {
      // Affect the way we close the channel and then rethrow exception.
      abortChannel = true;
      throw e;
    }
    finally {
      // Channel clean up
      if (channel != null) {
        try {
          // Close the channel one way or the other.
          if (abortChannel) channel.abort();
            else channel.close();
        } 
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", channel.getChannelNumber(), 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
    }
  }

  /* ---------------------------------------------------------------------- */
  /* postRecoveryQueue:                                                     */
  /* ---------------------------------------------------------------------- */
  /** Write a json message to the named tenant queue.  The queue name is used 
   * as the routing key on the direct exchange.
   * 
   * @param queueName the target queue name
   * @param message a json string
   */
  public void postRecoveryQueue(String message)
    throws JobException
  {
    // Get the non-null tenantId from the context.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    String tenantId = "xxx";
    
    // Don't allow processing for an uninitialized tenant.
    if (TapisThreadContext.INVALID_ID.equals(tenantId)) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_MISSING_TENANT_ID", "recoveryQueue");
        _log.error(msg);
        throw new JobException(msg);
    }
    
    // Get the exchange and queuenames.
    String queueName    = getTenantRecoveryQueueName(tenantId);
    String exchangeName = getTenantRecoveryExchangeName(tenantId); 
    
    // Create a temporary channel.
    Channel channel = null;
    boolean abortChannel = false;
    try {
      // Create a temporary channel.
      try {channel = getNewOutChannel();}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", tenantId);
          _log.error(msg, e);
          throw e;
        }
    
      // Publish the message to the queue.
      try {
        // Write the job to the tenant recovery queue.
        channel.basicPublish(exchangeName, DEFAULT_BINDING_KEY, QueueManager.PERSISTENT_JSON, 
                             message.getBytes("UTF-8"));
        
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_POST", tenantId, exchangeName, queueName);
            _log.debug(msg);
        }
      }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_PUBLISH_ERROR", exchangeName, 
                                       getOutConnectionName(), channel.getChannelNumber(), 
                                       e.getMessage());
          _log.error(msg, e);
          throw new JobQueueException(msg, e);
        }
    } 
    catch (Exception e) {
      // Affect the way we close the channel and then rethrow exception.
      abortChannel = true;
      throw e;
    }
    finally {
      // Channel clean up
      if (channel != null) {
        try {
          // Close the channel one way or the other.
          if (abortChannel) channel.abort();
            else channel.close();
        } 
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", channel.getChannelNumber(), 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
    }
  }

  /* ---------------------------------------------------------------------- */
  /* postDeadLetterQueue:                                                   */
  /* ---------------------------------------------------------------------- */
  /** Write a json message to the named tenant queue.  The queue name is used 
   * as the routing key on the direct exchange.
   * 
   * @param queueName the target queue name
   * @param message a json string
   */
  public void postDeadLetterQueue(String message)
    throws JobException
  {
    // Get the exchange and queuenames.
    String queueName    = getAllTenantDeadLetterQueueName();
    String exchangeName = getAllTenantDeadLetterExchangeName(); 
    
    // Create a temporary channel.
    Channel channel = null;
    boolean abortChannel = false;
    try {
      // Create a temporary channel.
      try {channel = getNewOutChannel();}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", "AllTenants");
          _log.error(msg, e);
          throw e;
        }
    
      // Publish the message to the queue.
      try {
        // Write the job to the tenant recovery queue.
        channel.basicPublish(exchangeName, DEFAULT_BINDING_KEY, QueueManager.PERSISTENT_TEXT, 
                             message.getBytes("UTF-8"));
        
        // Tracing.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_POST", "AllTenants", exchangeName, queueName);
            _log.debug(msg);
        }
      }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_PUBLISH_ERROR", exchangeName, 
                                       getOutConnectionName(), channel.getChannelNumber(), 
                                       e.getMessage());
          _log.error(msg, e);
          throw new JobQueueException(msg, e);
        }
    } 
    catch (Exception e) {
      // Affect the way we close the channel and then rethrow exception.
      abortChannel = true;
      throw e;
    }
    finally {
      // Channel clean up
      if (channel != null) {
        try {
          // Close the channel one way or the other.
          if (abortChannel) channel.abort();
            else channel.close();
        } 
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", channel.getChannelNumber(), 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
    }
  }

  /* ---------------------------------------------------------------------- */
  /* postCmdToAllWorkers:                                                   */
  /* ---------------------------------------------------------------------- */
  /** Post a command that is read by all workers.
   * 
   * @param cmdMsg the command
   * @throws JobException on error
   */
  public void postCmdToAllWorkers(CmdMsg cmdMsg)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(cmdMsg);
      
      // Get the tenant id, command topic name and all worker routing key.
      TapisThreadContext context = TapisThreadLocal.tapisThreadContext.get();
      String exchangeName  = getTenantCmdExchangeName("xxx");
      String routingKey = getCmdAllWorkerRoutingKey();
      
      // Call the actual post routine.
      postTenantTopic(exchangeName, json, routingKey);
  }
  
  /* ---------------------------------------------------------------------- */
  /* postCmdToWorker:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Post a command that is read by a specific worker.
   * 
   * @param cmdMsg the command
   * @throws JobException on error
   */
  public void postCmdToWorker(CmdMsg cmdMsg, String workerUuid)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(cmdMsg);
      
      // Get the tenant id, command topic name and specific worker routing key.
      TapisThreadContext context = TapisThreadLocal.tapisThreadContext.get();
      String exchangeName  = getTenantCmdExchangeName("xxx");
      String routingKey = getCmdSpecificWorkerRoutingKey(workerUuid);
      
      // Call the actual post routine.
      postTenantTopic(exchangeName, json, routingKey);
  }
  
  /* ---------------------------------------------------------------------- */
  /* postCmdToJob:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Post a command that is read by the worker currently servicing a specific
   * job, if such a worker exists.
   * 
   * @param cmdMsg the command
   * @throws JobException on error
   */
  public void postCmdToJob(CmdMsg cmdMsg, String jobUuid)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(cmdMsg);
      
      // Get the tenant id, command topic name and specific job routing key.
      TapisThreadContext context = TapisThreadLocal.tapisThreadContext.get();
      String exchangeName  = getTenantCmdExchangeName("xxx");
      String routingKey = getCmdSpecificJobRoutingKey(jobUuid);
      
      // Call the actual post routine.
      postTenantTopic(exchangeName, json, routingKey);
  }
  
  /* ---------------------------------------------------------------------- */
  /* postEventForAllSubscribers:                                            */
  /* ---------------------------------------------------------------------- */
  /** Post an event that is read by all event topic subscribers.
   * 
   * @param eventMsg the event 
   * @throws JobException on error
   */
  public void postEventForAllSubscribers(EventMsg eventMsg)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(eventMsg);
      
      // Get the tenant id, command topic name and all worker routing key.
      TapisThreadContext context = TapisThreadLocal.tapisThreadContext.get();
      String exchangeName  = getTenantEventExchangeName("xxx");
      String routingKey = getEventAllSubscriberRoutingKey();
      
      // Call the actual post routine.
      postTenantTopic(exchangeName, json, routingKey);
  }
  
  /* ---------------------------------------------------------------------- */
  /* postEventForSubscriber:                                                */
  /* ---------------------------------------------------------------------- */
  /** Post an event that is read by event topic subscribers interested in
   * a particular correlation id.  Correlation ids can be set by the sender
   * of a command message.  If the command generates events and the 
   * correlation is set, the routing key will incorporate the correlation id.
   * If the correlation id is not set, the all-subscriber routing is used. 
   * 
   * @param eventMsg the event 
   * @throws JobException on error
   */
  public void postEventForSubscriber(EventMsg eventMsg, String correlationId)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(eventMsg);
      
      // Get the tenant id, command topic name and all worker routing key.
      TapisThreadContext context = TapisThreadLocal.tapisThreadContext.get();
      String exchangeName  = getTenantEventExchangeName("xxx");
      String routingKey = getEventSpecificSubscriberRoutingKey(correlationId);
      
      // Call the actual post routine.
      postTenantTopic(exchangeName, json, routingKey);
  }
  
  /* ---------------------------------------------------------------------- */
  /* postEventForJob:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Post an event that is read by event topic subscribers interested in 
   * events concerning a particular job.
   * 
   * @param eventMsg the event 
   * @throws JobException on error
   */
  public void postEventForJob(EventMsg eventMsg, String jobUuid)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(eventMsg);
      
      // Get the tenant id, command topic name and all worker routing key.
      TapisThreadContext context = TapisThreadLocal.tapisThreadContext.get();
      String exchangeName  = getTenantEventExchangeName("xxx");
      String routingKey = getEventSpecificJobRoutingKey(jobUuid);
      
      // Call the actual post routine.
      postTenantTopic(exchangeName, json, routingKey);
  }
  
  /* ---------------------------------------------------------------------- */
  /* postRecoveryQueue:                                                     */
  /* ---------------------------------------------------------------------- */
  /** Post a recovery command to the recovery queue.
   * 
   * @param recoverMsg the recovery command
   * @throws JobException on error
   */
  public void postRecoveryQueue(RecoverMsg recoverMsg)
    throws JobException
  {
      // Convert command object to a json string.
      String json = TapisGsonUtils.getGson().toJson(recoverMsg);
      
      // Call the actual post routine.
      postRecoveryQueue(json);
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantExchangeArgs:                                                 */
  /* ---------------------------------------------------------------------- */
  /** Get the configuration arguments for the standard exchanges created for
   * tenants.  Using these arguments both senders and receivers can create the
   * exchange since it will be created in exactly the same way in both cases.  
   * 
   * @param tenantId the tenant who is creating an exchange
   * @return the exchange configuration arguments
   */
  public Map<String,Object> getTenantExchangeArgs(String exchangeName, String tenantId)
  {
      // Create the argument mapping.
      HashMap<String,Object> args = new HashMap<>();
      
      // Special case AllTenants.
      if (ALL_TENANTS_NAME.equals(tenantId)) {
          if (exchangeName.endsWith(MULTI_TENANT_ALT_EXCHANGE_SUFFIX))
              args.put("x-dead-letter-exchange", getAllTenantDeadLetterExchangeName());
          return args;
      }
      
      args.put("x-dead-letter-exchange", getAllTenantDeadLetterExchangeName());
      args.put("alternate-exchange", getAllTenantAltExchangeName());
      return args;
  }
  
  /* ---------------------------------------------------------------------- */
  /* unbindWorkerSpecificCmdTopic:                                          */
  /* ---------------------------------------------------------------------- */
  /** Remove a specific worker's binding from its tenant's command exchange.
   * This method makes a best effort attempt to unbind the topic from the 
   * exchange and logs an error in case of failure.  In all cases, the newly
   * created channel is closed.
   * 
   * @param tenantId the worker's tenant
   * @param wkrName the worker's name assigned at startup
   * @param workerUuid the worker's uuid
   * @param channel a channel on which to issue the command 
   */
  public void unbindWorkerSpecificCmdTopic(String tenantId, String wkrName, String workerUuid)
  {
      // Create the names needed to unbind a queue from an exchange. 
      String queue      = getTenantCmdTopicName(tenantId, wkrName);
      String exchange   = getTenantCmdExchangeName(tenantId);
      String bindingKey = getCmdSpecificWorkerBindingKey(workerUuid);
      
      // Get a new channel.  This method always closes the channel.
      Channel channel = null;
      try {channel = getNewOutChannel();}
      catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_Q_UNBIND_ERROR", "topic", 
                                       queue, bindingKey, exchange, e.getMessage());
          _log.error(msg, e);
          return;
      }
      
      // Unbind.
      try {channel.queueUnbind(queue, exchange, bindingKey);}
      catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_Q_UNBIND_ERROR", "topic", 
                                       queue, bindingKey, exchange, e.getMessage());
          _log.error(msg, e);
      }
      
      // Close the just created channel.
      try {channel.close();} 
      catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", 
                                       channel.getChannelNumber(), e.getMessage());
          _log.warn(msg, e);
      }
  }
  
  /* ---------------------------------------------------------------------- */
  /* createAndBindJobSpecificTopic:                                         */
  /* ---------------------------------------------------------------------- */
  /** Create and bind a job-specific topic to the tenant's command exchange.
   * 
   * @param tenantId the job's tenant
   * @param jobUuid the job's uuid
   * @throws JobQueueException
   */
  public void createAndBindJobSpecificTopic(Channel channel, String exchangeName, 
                                            String topicName, String bindingKey)
   throws JobQueueException
  {
      // Set the options for a transient, job-specific topic.
      boolean durable    = false;
      boolean exclusive  = false;
      boolean autoDelete = true;

      // Create the topic.
      createAndBindQueue(channel, exchangeName, topicName,
                         bindingKey, durable, exclusive, autoDelete);
  }
 
  /* ---------------------------------------------------------------------- */
  /* cancelConsumer:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Cancel the specified consumer on the channel.  If the queue being read
   * is configured with autoDelete, then the queue should be deleted as a 
   * result of this operation if there are no more subscribers reading the
   * queue.  
   * 
   * In general, this method should only be used when there's a need at
   * some later time to reuse the channel.  Otherwise, closing the channel will
   * release more resources and also cause autoDelete processing.
   * 
   * @param channel the channel on which basicConsume has been called
   * @param consumerTag the consumer's id
   */
  public void cancelConsumer(Channel channel, String consumerTag, String queueName)
   throws JobQueueException
  {
      // Don't blow up.
      if (channel == null || consumerTag == null) return;
      
      // Cancel the consumer on the provided channel.
      try {channel.basicCancel(consumerTag);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CANCEL_TOPIC_CONSUMER", 
                                         channel.getChannelNumber(), consumerTag,
                                         queueName, e.getMessage());
            _log.error(msg, e);
            throw new JobQueueException(msg, e);
          }
  }
  
  /* ---------------------------------------------------------------------- */
  /* dumpMessageInfo:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Construct a delivery message and threading information.  This method
   * should only be called after checking that debugging is enabled.
   * 
   * @param consumerTag the tag associated with the receiving consumer
   * @param envelope the message envelope
   * @param properties the message properties
   * @param body the message
   */
  public String dumpMessageInfo(String consumerTag, Envelope envelope, 
                                AMQP.BasicProperties properties, byte[] body)
  {
      // We assume all input parameters are non-null.
      Thread curthd = Thread.currentThread();
      ThreadGroup curgrp = curthd.getThreadGroup();
      int bodyLen = body == null ? 0 : body.length;
      String msg = "\n------------------------- Bytes Received: " + bodyLen + "\n";
      msg += "Consumer tag: " + consumerTag + "\n";
      msg += "Thread(name=" +curthd.getName() + ", isDaemon=" + curthd.isDaemon() + ")\n";
      msg += "ThreadGroup(name=" + curgrp.getName() + ", parentGroup=" + curgrp.getParent().getName() +
                  ", activeGroupCount=" + curgrp.activeGroupCount() + ", activeThreadCount=" + 
                  curgrp.activeCount() + ", isDaemon=" + curgrp.isDaemon() + ")\n";
      
      // Output is truncated at array size.
      Thread[] thdArray = new Thread[200];
      int thdArrayLen = curgrp.enumerate(thdArray, false); // non-recursive 
      msg += "ThreadArray(length=" + thdArrayLen + ", names=";
      for (int i = 0; i < thdArrayLen; i++) msg += thdArray[i].getName() + ((i < thdArrayLen-1) ? ", " : "");
      msg += ")\n";
      
      // Output is truncated at array size.
      ThreadGroup[] grpArray = new ThreadGroup[200];
      int grpArrayLen = curgrp.enumerate(grpArray, false); // non-recursive 
      msg += "ThreadGroupArray(length=" + grpArrayLen + ", names=";
      for (int i = 0; i < grpArrayLen; i++) msg += grpArray[i].getName() + ((i < grpArrayLen-1) ? ", " : "");
      msg += ")\n";
      
      // Avoid blowing up.
      if (envelope != null) msg += envelope.toString() + "\n";
      if (properties != null) {
          StringBuilder buf = new StringBuilder(512);
          properties.appendPropertyDebugStringTo(buf);
          msg += "Properties" + buf.toString() + "\n";
      }
      msg += "-------------------------------------------------\n";
      return msg;
  }

  /* ---------------------------------------------------------------------- */
  /* closeConnections:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Close all connections managed by this queue manager.  Use -1 for 
   * unlimited wait time.
   * 
   * @param timeoutMs milliseconds to wait before forcing connection closed
   * */
  public void closeConnections(int timeoutMs)
  {
      // Close each connection.
      if (_inConnection != null) 
          try {_inConnection.close(timeoutMs);}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_CLOSE_CONN_ERROR", "inbound", e.getMessage());
              _log.error(msg, e);
          }
      if (_outConnection != null) 
          try {_outConnection.close(timeoutMs);}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_CLOSE_CONN_ERROR", "outbound", e.getMessage());
              _log.error(msg, e);
          }
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getOutConnection:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Return a outbound connection to the queuing subsystem, creating the 
   * connection if necessary.
   * 
   * @return the connection
   * @throws JobQueueException on error.
   */
  private Connection getOutConnection()
   throws JobQueueException
  {
      // Create the connection if necessary.
      if (_outConnection == null)
      {
        // Only allow one thread at a time to create a shared connection.
        synchronized(QueueManager.class) {
          // Don't do anything if another thread beat us to the punch.
          if (_outConnection == null)
            try {_outConnection = getConnectionFactory().newConnection(getOutConnectionName());}
            catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_CONNECTION_CREATE_ERROR", e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
            } 
            catch (TimeoutException e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_CONNECTION_TIMEOUT_ERROR", e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
            }
        } // synchronized
      }
      
      return _outConnection;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getInConnection:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Return a inbound connection to the queuing subsystem, creating the 
   * connection if necessary.
   * 
   * @return the connection
   * @throws JobQueueException on error.
   */
  private Connection getInConnection()
   throws JobQueueException
  {
    // Create the connection if necessary.
    if (_inConnection == null)
    {
      // Only allow one thread at a time to create a shared connection.
      synchronized(QueueManager.class) {
        // Don't do anything if another thread beat us to the punch.
        if (_inConnection == null)
          try {_inConnection = getConnectionFactory().newConnection(getInConnectionName());}
          catch (IOException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CONNECTION_CREATE_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new JobQueueException(msg, e);
          } 
          catch (TimeoutException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CONNECTION_TIMEOUT_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new JobQueueException(msg, e);
          }
      } // synchronized
    }
    
    return _inConnection;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getConnectionFactory:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Return a connection factory, creating it if necessary.  The calling
   * method is required to handle multithread synchronization issues.  This
   * method assumes it is executed by only one thread at a time.s
   * 
   * @return this scheduler's queue connection factory.
   */
  private ConnectionFactory getConnectionFactory()
  {
      // Create the factory if necessary.
      if (_factory == null) 
      {
          // Get a rabbitmq connection factory.
          _factory = new ConnectionFactory();
          
          // Set the factory parameters.
          // TODO: generalize w/auth & network info & heartbeat
          RuntimeParameters parms = RuntimeParameters.getInstance();
          _factory.setHost(parms.getQueueHost());
          _factory.setPort(parms.getQueuePort());
          _factory.setUsername(parms.getQueueUser());
          _factory.setPassword(parms.getQueuePassword());
          _factory.setAutomaticRecoveryEnabled(parms.isQueueAutoRecoveryEnabled());
      }
      
      return _factory;
  }

  /* ---------------------------------------------------------------------- */
  /* createStandardQueues:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Create each tenant's alternate route and dead letter exchanges and queues.
   * 
   * @throws TapisException on error
   */
  @SuppressWarnings("unchecked")
  private void createStandardQueues(List<String> tenantIds) 
   throws TapisException
  {
    // Create the multi-tenant queues.
    createStandardMultiTenantQueues();
      
    // Process each tenant.
    for (String tenantId : tenantIds) {
      
      // Create a channel for each tenant.
      boolean channelAborted = false;
      Channel channel = null;
      try {
        // Create a temporary channel.
        try {channel = getNewInChannel();}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", tenantId);
            _log.error(msg, e);
            throw e;
          }
        
        // Create the command exchange without creating and binding a topic,
        // which will be handled by workers themselves.
        //
        // Configure the dead letter and alternate queues on this and 
        // all subsequent exchanges.
        HashMap<String,Object> exchangeArgs = new HashMap<>();
        exchangeArgs.put("x-dead-letter-exchange", getAllTenantDeadLetterExchangeName());
        exchangeArgs.put("alternate-exchange", getAllTenantAltExchangeName());
        createExchangeAndQueue(channel, tenantId, 
                               getTenantCmdExchangeName(tenantId), BuiltinExchangeType.TOPIC, 
                               null, null, 
                               (HashMap<String, Object>) exchangeArgs.clone());
        
        // Create the recovery exchange and queue and bind them together.
        createExchangeAndQueue(channel, tenantId, 
                               getTenantRecoveryExchangeName(tenantId), BuiltinExchangeType.FANOUT, 
                               getTenantRecoveryQueueName(tenantId), DEFAULT_BINDING_KEY, 
                               (HashMap<String, Object>) exchangeArgs.clone());
        
        // Create the event exchange and topic and bind them together.
        createExchangeAndQueue(channel, tenantId, 
                               getTenantEventExchangeName(tenantId), BuiltinExchangeType.TOPIC, 
                               getTenantEventTopicName(tenantId), getEventAllSubscriberBindingKey(), 
                               (HashMap<String, Object>) exchangeArgs.clone());
      }
      catch (Exception e) {
        if (channel != null) {
          try {channel.abort(AMQP.CHANNEL_ERROR, e.getMessage());} 
            catch (Exception e1){
              String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_ABORT_ERROR", 
                                           channel.getChannelNumber(), e1.getMessage());
              _log.warn(msg, e1);
            }
          // Don't try to close the channel again.
          channelAborted = true;
        }
        
        // Any exception causes the method to terminate.
        throw e;
      }
      finally {
        // Close the channel if it exists and hasn't already been aborted.
        if ((channel != null) && !channelAborted)
          try {channel.close();} 
              catch (Exception e1){
              String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", 
                                           channel.getChannelNumber(), e1.getMessage());
              _log.warn(msg, e1);
          }
      }
    } // for
  }
  
  /* ---------------------------------------------------------------------- */
  /* createStandardMultiTenantQueue:                                        */
  /* ---------------------------------------------------------------------- */
  /** Create the exchanges and queues service all tenants in an installation.
   * These queue objects use the pseudo-tenant id "AllTenants", which should
   * never name an actual tenant and we treat as a reserved name.  
   * 
   * Note: We don't currently check for a tenant named "AllTenants".
   * 
   * @throws TapisException
   */
  @SuppressWarnings("unchecked")
  private void createStandardMultiTenantQueues()
   throws TapisException
  {
      String allTenantId = QueueManager.ALL_TENANTS_NAME;
      Channel channel = null;
      try {
          // Create a temporary channel.
          try {channel = getNewInChannel();}
            catch (Exception e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", allTenantId);
              _log.error(msg, e);
              throw e;
            }
          
          // Create the dead letter exchange and queue and bind them together.
          createExchangeAndQueue(channel, allTenantId, 
                                 getAllTenantDeadLetterExchangeName(), BuiltinExchangeType.FANOUT, 
                                 getAllTenantDeadLetterQueueName(), DEFAULT_BINDING_KEY, null);
          
          // Create the alternate exchange and queue and bind them together.  
          // Configure the dead letter queue on this exchange.
          HashMap<String,Object> exchangeArgs = new HashMap<>();
          exchangeArgs.put("x-dead-letter-exchange", getAllTenantDeadLetterExchangeName());
          createExchangeAndQueue(channel, allTenantId, 
                                 getAllTenantAltExchangeName(), BuiltinExchangeType.FANOUT, 
                                 getAllTenantAltQueueName(), DEFAULT_BINDING_KEY, 
                                 (HashMap<String,Object>) exchangeArgs.clone());
      }
      finally {
          // Close the channel if it exists and hasn't already been aborted.
          if (channel != null)
            try {channel.close();} 
                catch (Exception e1){
                String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", 
                                             channel.getChannelNumber(), e1.getMessage());
                _log.warn(msg, e1);
            }
        }
  }
  
  /* ---------------------------------------------------------------------- */
  /* createExchangeAndQueue:                                                */
  /* ---------------------------------------------------------------------- */
  /** Create the named exchange, the named queue, and bind them.
   * 
   * @param channel the communication channel
   * @param tenantId the tenant whose components are being configured
   * @param exchangeName the exchange to create
   * @param exchangeType the type of exchange
   * @param queueName the queue to create or null if no queue should be created
   * @param bindingKey the key to use when binding the queue to the exchange
   * @throws JobQueueException on error
   */
  private void createExchangeAndQueue(Channel channel, String tenantId, 
                                      String exchangeName, BuiltinExchangeType exchangeType,
                                      String queueName, String bindingKey, 
                                      Map<String,Object> exchangeArgs) 
   throws JobQueueException
  {
      // Establish a durable exchange for the tenant.
      // Create the durable, non-autodelete topic exchange.
      boolean durable = true;
      boolean autodelete = false;
      try {channel.exchangeDeclare(exchangeName, exchangeType, durable, autodelete, exchangeArgs);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_XCHG_TENANT_ERROR", tenantId, 
                                            getInConnectionName(), channel.getChannelNumber(), 
                                            e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
      
      // Worker processes create and bind their own queues using their runtime 
      // name parameter.  In these cases, there's no more work to do.
      if (queueName == null) return;
      
      // Create the durable queue or topic with a well-known name.
      durable = true;
      boolean exclusive = false;
      boolean autoDelete = false;
      try {channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_Q_DECLARE_ERROR", exchangeType.getType(), 
                                           queueName, getInConnectionName(), 
                                           channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
      
      // Bind the queue/topic to the exchange with the binding key.
      try {channel.queueBind(queueName, exchangeName, bindingKey);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_Q_BIND_ERROR", exchangeType.getType(), 
                                           bindingKey, queueName, getInConnectionName(), 
                                           channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
  }
  
  /* ---------------------------------------------------------------------- */
  /* createTenantDefinedQueues:                                             */
  /* ---------------------------------------------------------------------- */
  /** Retrieve all queue defined in the database for each tenant and create  
   * the queues on the broker.  Exchanges are also defined.
   * @throws JobQueueException 
   * 
   */
//  private void createTenantDefinedQueues() throws JobQueueException
//  {
//    // Get all the queues defined in the system.
//    Map<String,List<JobQueue>> queueMap = TenantQueueMapping.getAllQueueMap();
//    
//    // Create each queue.  Queue creation is idempotent as 
//    // long as the configuration and options don't change.
//    for (Entry<String, List<JobQueue>> entry : queueMap.entrySet()) {
//      
//      // Create a channel for each tenant.
//      boolean channelAborted = false;
//      Channel channel = null;
//      try {
//        // Create a temporary channel.
//        try {channel = getNewOutChannel();}
//          catch (Exception e) {
//            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_TENANT_ERROR", entry.getKey());
//            _log.error(msg, e);
//            throw e;
//          }
//      
//        // Create an exchange for each tenant and  
//        // bind all the tenants queues to it.
//        for (JobQueue queue : entry.getValue()) {
//        
//          // Get the tenantId.
//          String tenantId = entry.getKey();
//          String exchangeName = getTenantSubmitExchangeName(tenantId); 
//        
//          // Create the tenant exchange.
//          boolean durable = true;
//          boolean autodelete = false;
//          try {channel.exchangeDeclare(exchangeName, "direct", durable, autodelete,
//                                       getTenantExchangeArgs(exchangeName, tenantId));}
//            catch (IOException e) {
//                String msg = MsgUtils.getMsg("JOBS_QMGR_XCHG_TENANT_ERROR", tenantId, 
//                                              getOutConnectionName(), channel.getChannelNumber(), 
//                                              e.getMessage());
//                _log.error(msg, e);
//                throw new JobQueueException(msg, e);
//            }
//        
//          // Create and bind queue to exchange.
//          createAndBindQueue(channel, exchangeName, queue.getName());
//        }
//      }
//      catch (Exception e) {
//        if (channel != null) {
//          try {channel.abort(AMQP.CHANNEL_ERROR, e.getMessage());} 
//            catch (Exception e1){
//              String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_ABORT_ERROR", 
//                                           channel.getChannelNumber(), e1.getMessage());
//              _log.warn(msg, e1);
//            }
//          // Don't try to close the channel again.
//          channelAborted = true;
//        }
//        
//        // Any exception causes the method to terminate.
//        throw e;
//      }
//      finally {
//        // Close the channel if it exists and hasn't already been aborted.
//        if ((channel != null) && !channelAborted)
//          try {channel.close();} 
//              catch (Exception e1){
//              String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", 
//                                           channel.getChannelNumber(), e1.getMessage());
//              _log.warn(msg, e1);
//          }
//      }
//    }
//  }
  
  /* ---------------------------------------------------------------------- */
  /* createAndBindQueue:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Create a tenant queue and bind it to the specified direct exchange.
   * The binding key is the queue name. 
   * 
   * @param channel the channel used to post to the queue
   * @param exchangeName the target exchange
   * @param queueName the queue to be posted
   * @throws JobQueueException on error
   */
  private void createAndBindQueue(Channel channel, String exchangeName, String queueName)
    throws JobQueueException
  {
      // Set the standard queue/topic options.
      boolean durable = true;
      boolean exclusive = false;
      boolean autoDelete = false;
      
      // Create the queue with the configured name.
      createAndBindQueue(channel, exchangeName, queueName, null, durable, exclusive, autoDelete);
  }
  
  /* ---------------------------------------------------------------------- */
  /* createAndBindQueue:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Create a tenant queue and bind it to the specified direct exchange.
   * The binding key is the queue name. 
   * 
   * @param channel the channel used to post to the queue
   * @param exchangeName the target exchange
   * @param queueName the queue to be posted
   * @param bindingKey the key to bind the queue to the exchange or null to use queueName
   * @param durable whether the queue will survive a broker restart
   * @param exclusive whether used by only one connection and the queue will be deleted when that connection closes
   * @param autoDelete whether queue that has had at least one consumer is deleted when last consumer unsubscribes
   * @throws JobQueueException on error
   */
  private void createAndBindQueue(Channel channel, String exchangeName, String queueName, String bindingKey, 
                                  boolean durable, boolean exclusive, boolean autoDelete)
    throws JobQueueException
  {
    // Create the queue with the configured name.
    try {channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);}
        catch (IOException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_Q_DECLARE_ERROR", "queue", 
                                         queueName, getOutConnectionName(), 
                                         channel.getChannelNumber(), e.getMessage());
            _log.error(msg, e);
            throw new JobQueueException(msg, e);
        }
   
    // Bind the queue to the exchange using the queue name as the 
    // binding key if the caller hasn't specified a key.
    if (bindingKey == null) bindingKey = queueName;
    try {channel.queueBind(queueName, exchangeName, bindingKey);}
        catch (IOException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_Q_BIND_ERROR", "queue", queueName, 
                                         bindingKey, getOutConnectionName(), 
                                         channel.getChannelNumber(), e.getMessage());
            _log.error(msg, e);
            throw new JobQueueException(msg, e);
        }
  }
  
}

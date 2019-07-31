package edu.utexas.tacc.tapis.sharedq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedq.exceptions.TapisQueueException;

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
  private static QueueManager     _instance;
  
  // Configuration parameters.
  private final QueueManagerParms _parms;
  
  // Fields that get initialized once and tend not to change.
  private ConnectionFactory       _factory;
  private Connection              _outConnection;
  private Connection              _inConnection;

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
  private QueueManager(QueueManagerParms parms) 
   throws TapisException
  {
      // Make sure we have a parameter object.
      if (parms == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "QueueManager", 
                                       "parms");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Validate the parameters.
      parms.validate();
      
      // Set the parms for the singleton.
      _parms = parms;
      
      // Create the multi-tenant queues.
      try {createStandardMultiTenantQueues();}
      catch (Exception e) {
          String msg = MsgUtils.getMsg("TAPIS_QMGR_INIT_ERROR", ALL_TENANTS_NAME);
          _log.error(msg, e);
      }
  }
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getInstance:                                                           */
  /* ---------------------------------------------------------------------- */
  /** Get the singleton instance of this class, creating it if necessary.
   * If the singleton already exists, the parameters are ignored.
   * 
   * @return the new or existing singleton.
 * @throws TapisException 
   */
  public static QueueManager getInstance(QueueManagerParms parms) 
   throws TapisException
  {
    // Create the singleton instance if it's null without
    // setting up a synchronized block in the common case.
    if (_instance == null) {
      synchronized (QueueManager.class) {
        if (_instance == null) _instance = new QueueManager(parms);
      }
    }
    return _instance;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getInstance:                                                           */
  /* ---------------------------------------------------------------------- */
  /** Get the singleton instance of this class.  Calling this method before
   * the singleton exists causes a runtime error.
   * 
   * @return the existing singleton
   */
  public static QueueManager getInstance()
  {
    // The singleton instance must have been created before
    // this method is called.
    if (_instance == null) {
        String msg = MsgUtils.getMsg("QMGR_UNINITIALIZED_ERROR");
        _log.error(msg);
        throw new TapisRuntimeException(msg);
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
    throws TapisQueueException
  {
      // Create a new channel in this phase's connection.
      Channel channel = null;
      try {channel = getOutConnection().createChannel();} 
       catch (IOException e) {
           String msg = MsgUtils.getMsg("QMGR_CHANNEL_CREATE_ERROR", 
                                        getOutConnectionName(), e.getMessage());
           _log.error(msg, e);
           throw new TapisQueueException(msg, e);
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
    throws TapisQueueException
  {
      // Create a new channel in this phase's connection.
      Channel channel = null;
      try {channel = getInConnection().createChannel();} 
       catch (IOException e) {
           String msg = MsgUtils.getMsg("QMGR_CHANNEL_CREATE_ERROR", 
                                        getOutConnectionName(), e.getMessage());
           _log.error(msg, e);
           throw new TapisQueueException(msg, e);
       }
      
      // Tracing.
      if (_log.isInfoEnabled()) 
          _log.info("Created channel number " + channel.getChannelNumber() + 
                    " on " + getInConnectionName() + ".");
       
      return channel;
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
    throws TapisQueueException
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
          String msg = MsgUtils.getMsg("QMGR_CHANNEL_TENANT_ERROR", ALL_TENANTS_NAME);
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
            String msg = MsgUtils.getMsg("QMGR_POST", ALL_TENANTS_NAME, exchangeName, queueName);
            _log.debug(msg);
        }
      }
        catch (Exception e) {
          String msg = MsgUtils.getMsg("QMGR_PUBLISH_ERROR", exchangeName, 
                                       getOutConnectionName(), channel.getChannelNumber(), 
                                       e.getMessage());
          _log.error(msg, e);
          throw new TapisQueueException(msg, e);
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
            String msg = MsgUtils.getMsg("QMGR_CHANNEL_CLOSE_ERROR", channel.getChannelNumber(), 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
    }
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
  /* createAndBindJobSpecificTopic:                                         */
  /* ---------------------------------------------------------------------- */
  /** Create and bind a job-specific topic to the tenant's command exchange.
   * 
   * @param tenantId the job's tenant
   * @param jobUuid the job's uuid
   * @throws JobQueueException
   */
  public void createAndBindSpecificTopic(Channel channel, String exchangeName, 
                                            String topicName, String bindingKey)
   throws TapisQueueException
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
   throws TapisQueueException
  {
      // Don't blow up.
      if (channel == null || consumerTag == null) return;
      
      // Cancel the consumer on the provided channel.
      try {channel.basicCancel(consumerTag);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("QMGR_CANCEL_TOPIC_CONSUMER", 
                                         channel.getChannelNumber(), consumerTag,
                                         queueName, e.getMessage());
            _log.error(msg, e);
            throw new TapisQueueException(msg, e);
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
              String msg = MsgUtils.getMsg("QMGR_CLOSE_CONN_ERROR", "inbound", e.getMessage());
              _log.error(msg, e);
          }
      if (_outConnection != null) 
          try {_outConnection.close(timeoutMs);}
          catch (Exception e) {
              String msg = MsgUtils.getMsg("QMGR_CLOSE_CONN_ERROR", "outbound", e.getMessage());
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
   throws TapisQueueException
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
              String msg = MsgUtils.getMsg("QMGR_CONNECTION_CREATE_ERROR", e.getMessage());
              _log.error(msg, e);
              throw new TapisQueueException(msg, e);
            } 
            catch (TimeoutException e) {
              String msg = MsgUtils.getMsg("QMGR_CONNECTION_TIMEOUT_ERROR", e.getMessage());
              _log.error(msg, e);
              throw new TapisQueueException(msg, e);
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
   throws TapisQueueException
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
            String msg = MsgUtils.getMsg("QMGR_CONNECTION_CREATE_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisQueueException(msg, e);
          } 
          catch (TimeoutException e) {
            String msg = MsgUtils.getMsg("QMGR_CONNECTION_TIMEOUT_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisQueueException(msg, e);
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
          _factory.setHost(_parms.getQueueHost());
          _factory.setPort(_parms.getQueuePort());
          _factory.setUsername(_parms.getQueueUser());
          _factory.setPassword(_parms.getQueuePassword());
          _factory.setAutomaticRecoveryEnabled(_parms.isQueueAutoRecoveryEnabled());
      }
      
      return _factory;
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
   * @throws AloeException
   */
  @SuppressWarnings("unchecked")
  private void createStandardMultiTenantQueues()
   throws TapisQueueException
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
                String msg = MsgUtils.getMsg("QMGR_CHANNEL_CLOSE_ERROR", 
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
   throws TapisQueueException
  {
      // Establish a durable exchange for the tenant.
      // Create the durable, non-autodelete topic exchange.
      boolean durable = true;
      boolean autodelete = false;
      try {channel.exchangeDeclare(exchangeName, exchangeType, durable, autodelete, exchangeArgs);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("QMGR_XCHG_TENANT_ERROR", tenantId, 
                                            getInConnectionName(), channel.getChannelNumber(), 
                                            e.getMessage());
              _log.error(msg, e);
              throw new TapisQueueException(msg, e);
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
              String msg = MsgUtils.getMsg("QMGR_Q_DECLARE_ERROR", exchangeType.getType(), 
                                           queueName, getInConnectionName(), 
                                           channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new TapisQueueException(msg, e);
          }
      
      // Bind the queue/topic to the exchange with the binding key.
      try {channel.queueBind(queueName, exchangeName, bindingKey);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("QMGR_Q_BIND_ERROR", exchangeType.getType(), 
                                           bindingKey, queueName, getInConnectionName(), 
                                           channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new TapisQueueException(msg, e);
          }
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
   * @throws JobQueueException on error
   */
  private void createAndBindQueue(Channel channel, String exchangeName, String queueName)
    throws TapisQueueException
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
    throws TapisQueueException
  {
    // Create the queue with the configured name.
    try {channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);}
        catch (IOException e) {
            String msg = MsgUtils.getMsg("QMGR_Q_DECLARE_ERROR", "queue", 
                                         queueName, getOutConnectionName(), 
                                         channel.getChannelNumber(), e.getMessage());
            _log.error(msg, e);
            throw new TapisQueueException(msg, e);
        }
   
    // Bind the queue to the exchange using the queue name as the 
    // binding key if the caller hasn't specified a key.
    if (bindingKey == null) bindingKey = queueName;
    try {channel.queueBind(queueName, exchangeName, bindingKey);}
        catch (IOException e) {
            String msg = MsgUtils.getMsg("QMGR_Q_BIND_ERROR", "queue", queueName, 
                                         bindingKey, getOutConnectionName(), 
                                         channel.getChannelNumber(), e.getMessage());
            _log.error(msg, e);
            throw new TapisQueueException(msg, e);
        }
  }
  
  /* ---------------------------------------------------------------------- */
  /* getOutConnectionName:                                                  */
  /* ---------------------------------------------------------------------- */
  public String getOutConnectionName()
  {return getOutConnectionName(_parms.getInstanceName());}
  
  /* ---------------------------------------------------------------------- */
  /* getInConnectionName:                                                   */
  /* ---------------------------------------------------------------------- */
  public String getInConnectionName()
  {return getInConnectionName(_parms.getInstanceName());}
}

package edu.utexas.tacc.tapis.jobs.worker;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedq.exceptions.TapisQueueException;

/** Base class for all worker processors that injest queue elements.
 * 
 * @author rcardone
 */
abstract class AbstractProcessor 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(AbstractProcessor.class);
  
  // Limits.
  protected static final int JSON_DUMP_LEN = 64;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // The top-level worker that spawned the thread on which we run.
  protected final JobWorker _jobWorker;
  
  // The local queue this thread waits on for elements read
  // from the remote queue by the RabbitMQ consumer.  This
  // approach allows this thread to perform the actual job
  // processing (as opposed to the RabbitMQ thread).
  private final ArrayBlockingQueue<DeliveryResponse> _deliveryQueue;
  
  // The private channel for this thread to the queue broker.
  protected Channel         _channel;
  
  // The consumer that reads message from the queue broker.
  protected Consumer        _consumer;
  
  // The consumer tag returned when the consumer is started.
  protected String          _consumerTag;
  
  // The queue or topic name initialized when a channel is created.
  protected String          _queueName;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  AbstractProcessor(JobWorker jobWorker)
  {
    // Get a back pointer to the main class instance.
    _jobWorker = jobWorker;
    
    // Create a queue with fixed capacity of 1 since
    // our channel will have a prefetch limit of 1.
    _deliveryQueue = new ArrayBlockingQueue<>(1);
  }
  
  /* ********************************************************************** */
  /*                            Abstract Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getNextMessage:                                                        */
  /* ---------------------------------------------------------------------- */
  /** This is the main queue read loop.  Subclasses initialize a NextMessageParms
   * object and then call the overloaded method in this class that takes that 
   * object.  The overloaded method acquires a channel and then calls readQueue() 
   * to get queued input.  Reads are blocking and at most one unacknowledged 
   * message is outstanding at a time.
   */
  protected abstract void getNextMessage();
  
  /* ---------------------------------------------------------------------- */
  /* process:                                                               */
  /* ---------------------------------------------------------------------- */
  /** This is the main processor method that subclasses implement to handle
   * incoming messages.  It is called from readQueue().
   * 
   * @param delivery the incoming message and its metadata
   * @return true if the message was successfully processed, false if the 
   *          message should be rejected and discarded without redelivery
   */
  protected abstract boolean process(DeliveryResponse delivery);
  
  /* ********************************************************************** */
  /*                           Protected Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getNextMessage:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Initiate the read loop for this processor using processor-specific
   * parameters.
   * 
   * There are 3 ways for this method to end:
   * 
   *  1. Normal exit on shutdown interrupt.
   *  2. AloeRuntimeException when we want a new thread to replace this one
   *      because of some problem that we have detected.
   *  3. Any other runtime exception, which will also cause a new thread
   *      to replace this one.
   * 
   * @param p read parameters specific to processor
   * @throws AloeRuntimeException runtime error that ends this thread and  
   *            starts a new replacement thread
   */
  protected void getNextMessage(NextMessageParms p)
  {
    // Tracing.
    if (_log.isDebugEnabled()) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_EXCHANGE_READER", _jobWorker.getParms().name,
                                     Thread.currentThread().getName(), TapisUtils.toString(p));
        _log.debug(msg);
    }
      
    // No matter what, we clean up the channel.
    try {
      // Initialize our channel.
      try {_channel = getChannel(p._exchangeName, p._exchangeType, p._queueName, p._bindingKeys);}
        catch (Exception e) {
          // This error terminates this thread.
          String msg = MsgUtils.getMsg("JOBS_WORKER_CHANNEL_INIT_ERROR", 
                                       _jobWorker.getParms().name,
                                       p._queueName, 
                                       e.getMessage());
          _log.error(msg, e);
          throw new TapisRuntimeException(msg, e);
      }
    
      // Enter the queue read loop. Runtime 
      // exceptions can be thrown from here. 
      readQueue();
    }
    finally {
      // Don't leave without cleaning up the channel.
      if (_channel != null && _channel.isOpen())
        try {_channel.close();}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", 
                _channel.getChannelNumber(), e.getMessage());
            _log.error(msg, e);
          }
    }
  }
  
  /* ---------------------------------------------------------------------- */
  /* getNextJobSpecificMessage:                                             */
  /* ---------------------------------------------------------------------- */
  /** Initiate the read loop for the job-specific command processor. This
   * processor dynamically creates a non-durable, autoDelete topic that
   * only exists when a job is actively processing. 
   * 
   * There are 3 ways for this method to end:
   * 
   *  1. Normal exit on shutdown interrupt.
   *  2. AloeRuntimeException when we want a new thread to replace this one
   *      because of some problem that we have detected.
   *  3. Any other runtime exception, which will also cause a new thread
   *      to replace this one.
   * 
   * @param p read parameters specific to processor
   * @throws AloeRuntimeException runtime error that ends this thread and  
   *            starts a new replacement thread
   */
  protected void getNextJobSpecificMessage(NextMessageParms p)
  {
    // Tracing.
    if (_log.isDebugEnabled()) {
        String msg = MsgUtils.getMsg("JOBS_QUEUE_EXCHANGE_READER", _jobWorker.getParms().name,
                                     Thread.currentThread().getName(), TapisUtils.toString(p));
        _log.debug(msg);
    }
      
    // No matter what, we clean up the channel..
    try {
      // Initialize our channel.
      try {_channel = getJobSpecificChannel(p._exchangeName, p._exchangeType, p._queueName, p._bindingKeys);}
        catch (Exception e) {
          // This error terminates this thread.
          String msg = MsgUtils.getMsg("JOBS_WORKER_CHANNEL_INIT_ERROR", 
                                       _jobWorker.getParms().name,
                                       p._queueName, 
                                       e.getMessage());
          _log.error(msg, e);
          throw new TapisRuntimeException(msg, e);
      }
    
      // Enter the queue read loop. Runtime 
      // exceptions can be thrown from here. 
      readQueue();
    }
    finally {
      // Don't leave without cleaning up the channel.
      if (_channel != null && _channel.isOpen())
        try {_channel.close();}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_CLOSE_ERROR", 
                _channel.getChannelNumber(), e.getMessage());
            _log.error(msg, e);
          }
    }
  }
  
  /* ---------------------------------------------------------------------- */
  /* getProcessorName:                                                      */
  /* ---------------------------------------------------------------------- */
  /** Construct a name appropriate for logging purposes that distinguishes 
   * this processor instance.
   * 
   * @return the distinguishing processor name
   */
  protected String getProcessorName()
  {
      return _jobWorker.getParms().name + "-" + this.getClass().getSimpleName();
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getChannel:                                                            */
  /* ---------------------------------------------------------------------- */
  /** One-time initialization of channel used by this thread to read the 
   * assigned queue or topic.  It is the caller's responsibility to close the
   * returned channel. 
   * 
   * @param exchangeName the non-null exchange name used by the channel
   * @param exchangeType "direct" or "topic" are the only ones used
   * @param queueName the non-null name of the queue to read
   * @param bindingKeys the non-empty list of binding key for the queue/topic
   * 
   * @return the initialized channel 
   * @throws JobException on error
   */
  private Channel getChannel(String exchangeName, BuiltinExchangeType exchangeType, 
                             String queueName, String[] bindingKeys)
    throws JobException, TapisQueueException
  {
    // Save the queue name for use in other methods.
    _queueName = queueName;
    
    // Create the channel.
    Channel channel = null;
    JobQueueManager qmgr = JobQueueManager.getInstance();
    
    try {
      // Create the channel.
      channel = qmgr.getNewInChannel();
      
      // Set the prefetch count so that the consumer using this 
      // channel only receives the next request after the previous
      // request has been acknowledged.
      final int prefetchCount = 1;
      try {channel.basicQos(prefetchCount);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_WORKER_CHANNEL_PREFETCH_ERROR", 
                              _jobWorker.getParms().name, 
                              channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
      
      // Create the exchange.
      final boolean durable = true;
      final boolean autodelete = false;
      try {channel.exchangeDeclare(exchangeName, exchangeType.getType(), durable, autodelete, 
                                   qmgr.getExchangeArgs());}
        catch (IOException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_XCHG_ERROR", exchangeName, 
                                          qmgr.getInConnectionName(), channel.getChannelNumber(), 
                                          e.getMessage());
            _log.error(msg, e);
            throw new JobQueueException(msg, e);
        }

      // Create the queue with the configured name.
      final boolean exclusive = false;
      final boolean autoDelete = false;
      try {channel.queueDeclare(_queueName, durable, exclusive, autoDelete, null);}
          catch (IOException e) {
              String qtype = (exchangeType == BuiltinExchangeType.TOPIC) ? "topic" : "queue";
              String msg = MsgUtils.getMsg("JOBS_QMGR_Q_DECLARE_ERROR", qtype, 
                                           _queueName, qmgr.getInConnectionName(), 
                                           channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
     
      // Bind the queue to the exchange using for each binding key.
      String lastBindingKey = null;
      try {
          for (String bindingKey : bindingKeys) {
            lastBindingKey = bindingKey;
            channel.queueBind(_queueName, exchangeName, bindingKey);}
          }
          catch (IOException e) {
              String qtype = (exchangeType == BuiltinExchangeType.TOPIC) ? "topic" : "queue";
              String msg = MsgUtils.getMsg("JOBS_QMGR_Q_BIND_ERROR", qtype, _queueName, 
                                           lastBindingKey, qmgr.getInConnectionName(), 
                                           channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
    }
    catch (Exception e) {
      if (channel != null) {
        try {channel.abort(AMQP.CHANNEL_ERROR, e.getMessage());} 
          catch (Exception e1){
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_ABORT_ERROR", 
                                         channel.getChannelNumber(), e1.getMessage());
            _log.warn(msg, e1);
          }
      }
      
      // Any exception causes the method to terminate.
      throw e;
    }
    
    return channel;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getChannel:                                                            */
  /* ---------------------------------------------------------------------- */
  /** One-time initialization of channel used by the job-specific command
   * processing thread.  This specialized method assumes that the the 
   * command exchange has already been set up for this tenant, so it does not
   * try to create it.
   * 
   * It is the caller's responsibility to close the returned channel.
   * 
   * @param exchangeName the non-null exchange name used by the channel
   * @param exchangeType "direct" or "topic" are the only ones used
   * @param queueName the non-null job-specific topic name of the queue to read
   * @param bindingKeys the list of exactly 1 binding key for the topic
   * 
   * @return the initialized channel 
   * @throws JobException on error
   */
  private Channel getJobSpecificChannel(String exchangeName, BuiltinExchangeType exchangeType, 
                                        String queueName, String[] bindingKeys)
    throws JobException, TapisQueueException
  {
    // Save the queue name for use in other methods.
    _queueName = queueName;
    
    // Create the channel.
    Channel channel = null;
    JobQueueManager qmgr = JobQueueManager.getInstance();
    
    try {
      // Create the channel.
      channel = qmgr.getNewInChannel();
      
      // Set the prefetch count so that the consumer using this 
      // channel only receives the next request after the previous
      // request has been acknowledged.
      final int prefetchCount = 1;
      try {channel.basicQos(prefetchCount);}
          catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_WORKER_CHANNEL_PREFETCH_ERROR", 
                              _jobWorker.getParms().name, 
                              channel.getChannelNumber(), e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
      
      // Create the job-specific, non-durable, autoDeleted topic.  This topic is deleted when the channel is closed. 
      try {qmgr.createAndBindAutoDeleteTopic(channel, exchangeName, queueName, bindingKeys[0]);}
          catch (Exception e) {
              // There's no point in continuing if we can't read the queue.
              String msg = MsgUtils.getMsg("JOBS_QUEUE_JOB_SPECIFIC_THREAD_BIND", Thread.currentThread().getName(),
                              _jobWorker.getParms().name, queueName, exchangeName, bindingKeys[0], e.getMessage());
              _log.error(msg, e);
              throw new JobQueueException(msg, e);
          }
    }
    catch (Exception e) {
      if (channel != null) {
        try {channel.abort(AMQP.CHANNEL_ERROR, e.getMessage());} 
          catch (Exception e1){
            String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_ABORT_ERROR", 
                                         channel.getChannelNumber(), e1.getMessage());
            _log.warn(msg, e1);
          }
      }
      
      // Any exception causes the method to terminate.
      throw e;
    }
    
    return channel;
  }
  
  /* ---------------------------------------------------------------------- */
  /* readQueue:                                                             */
  /* ---------------------------------------------------------------------- */
  /** Infinite read loop that can throw runtime exceptions.  This method
   * starts reading the remote queue with auto-ack turned off. It calls the 
   * subclass's process method to handle the input, which then sends an
   * ack or nack back to the broker depending the dispatch method's return 
   * code.
   * 
   * This is a blocking read call.  The infinite loop is broken if the thread 
   * is interrupted.
   */
  private void readQueue()
  {
    // Initialize the consumer.
    _consumer = createConsumer();
    
    // Start the consumer and throw exception on error.
    _consumerTag = startConsumer();
    
    // Save the thread's base logging identifier before 
    // it gets reassigned on each loop iteration.
    String baseId = MDC.get(TapisConstants.MDC_ID_KEY);
    
    // The queue read/job processing loop.
    while (!Thread.currentThread().isInterrupted())
    {
      // Reassign the logging identifier on each loop iteration
      // to distinguish between the processing of different requests. 
      MDC.put(TapisConstants.MDC_ID_KEY, TapisUtils.getRandomString());
        
      // Wait for a message to be delivered.
      DeliveryResponse delivery = null;
      try {delivery = _deliveryQueue.take();}
        catch (InterruptedException e) {
          // Set the interrupt bit for this thread
          // before breaking from the main loop.
          Thread.currentThread().interrupt();
          break;
        }
    
      // Let the subclass perform the actual message processing.
      boolean ack = process(delivery);
      
      // Acknowledge or reject the message.
      // Determine whether to ack or nack the request.
      if (ack) {
        // Don't forget to send the ack!
        final boolean multipleAck = false;
        try {_channel.basicAck(delivery.envelope.getDeliveryTag(), multipleAck);}
          catch (IOException e) {
            String msg = MsgUtils.getMsg("JOBS_THREAD_ACK_ERROR",
                                         Thread.currentThread().getName(),
                                         Thread.currentThread().getId(),
                                         _jobWorker.getParms().name,
                                         _queueName, 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
      else {
        // Reject this unreadable message so that
        // it gets discarded or dead-lettered.
        final boolean requeue = false;
        try {_channel.basicReject(delivery.envelope.getDeliveryTag(), requeue);} 
          catch (IOException e) {
            String msg = MsgUtils.getMsg("JOBS_THREAD_REJECT_ERROR",
                                         Thread.currentThread().getName(),
                                         Thread.currentThread().getId(),
                                         _jobWorker.getParms().name,
                                         _queueName, 
                                         e.getMessage());
            _log.error(msg, e);
          }
      }
    }
    
    // Reassign the thread's base logging id.
    if (baseId != null) MDC.put(TapisConstants.MDC_ID_KEY, baseId);
  }
  
  /* ---------------------------------------------------------------------- */
  /* createConsumer:                                                        */
  /* ---------------------------------------------------------------------- */
  /** This is the callback method that the queue client thread calls to handle
   * delivered messages.  This method packages the message received from the 
   * queue broker and places it on the internal queue.  The blocking 
   * readQueue() method reads the packaged input from the internal queue
   * and calls the concrete subclass's process message to handle the input. 
   * 
   * @return a message consumer object that implements the delivery handling method
   * 
   */
  private Consumer createConsumer()
  {
    // Create the topic queue consumer.
    Consumer consumer = new DefaultConsumer(_channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) 
        throws IOException 
      {
        // Package the response for the main thread.
        DeliveryResponse delivery = new DeliveryResponse();
        delivery.consumerTag = consumerTag;
        delivery.envelope    = envelope;
        delivery.properties  = properties;
        delivery.body        = body;
        
        // Queue the response locally.
        try {_deliveryQueue.put(delivery);}
          catch (InterruptedException e) {
            String msg = MsgUtils.getMsg("JOBS_THREAD_CONSUMER_INTERRUPTED",
                                          Thread.currentThread().getName(),
                                          Thread.currentThread().getId(),
                                          _jobWorker.getParms().name, 
                                          _queueName);
            _log.info(msg, e);    
          }
      }
    };
      
   return consumer;
  }
  
  /* ---------------------------------------------------------------------- */
  /* startConsumer:                                                         */
  /* ---------------------------------------------------------------------- */
  /** Start reading the configured queue using the previously defined consumer.  
   * This method allows traffic to begin flowing from the queue broker to
   * this thread.
   * 
   * @return the channel's consumer tag
   */
  private String startConsumer()
  {
    // We don't auto-acknowledge topic broadcasts.
    boolean autoack = false;
    String consumerTag = null;
    try {
        // Save the server generated tag for this consumer.  The tag can be used
        // as input on other APIs, such as basicCancel.
        consumerTag = _channel.basicConsume(_queueName, autoack, _consumer);
    }
    catch (Exception e) {
      String msg = MsgUtils.getMsg("JOBS_THREAD_CONSUMER_START_ERROR",
                                   Thread.currentThread().getName(),
                                   Thread.currentThread().getId(),
                                   _jobWorker.getParms().name,
                                   _queueName, 
                                   e.getMessage());
      _log.error(msg, e);
      throw new TapisRuntimeException(msg, e);
    }
    
    return consumerTag;
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /** Instances of this class hold the parameters used by subclasses when they
   * call the next message method.
   */
  protected final class NextMessageParms
  {
      // Fields.
      protected String              _exchangeName;
      protected BuiltinExchangeType _exchangeType;
      protected String              _queueName;
      protected String[]            _bindingKeys;
      
      // Constructor.
      protected NextMessageParms(String exchangeName, BuiltinExchangeType exchangeType,
                                 String queueName, String[] bindingKeys)
      {
          _exchangeName = exchangeName;
          _exchangeType = exchangeType;
          _queueName    = queueName;
          _bindingKeys  = bindingKeys;
      }
  }
}

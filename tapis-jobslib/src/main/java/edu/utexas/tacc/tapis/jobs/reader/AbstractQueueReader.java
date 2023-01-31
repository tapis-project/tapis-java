package edu.utexas.tacc.tapis.jobs.reader;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueException;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager.ExchangeUse;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public abstract class AbstractQueueReader 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractQueueReader.class);

    // Limits.
    protected static final int JSON_DUMP_LEN = 64;
    protected static final int MAX_BODY_PREFIX_LEN = 4096;

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Input parameters.
    protected final QueueReaderParameters _parms;
    
    // The local queue this thread waits on for elements read
    // from the remote queue by the RabbitMQ consumer.  This
    // approach allows this thread to perform the actual job
    // processing (as opposed to the RabbitMQ thread).
    private final ArrayBlockingQueue<DeliveryResponse> _deliveryQueue;
    
    // The private channel for this thread to the queue broker.
    private Channel         _channel;
    
    // The consumer that reads message from the queue broker.
    private Consumer        _consumer;
    
    // The consumer tag returned when the consumer is started.
    private String          _consumerTag;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected AbstractQueueReader(QueueReaderParameters parms)
      throws TapisRuntimeException
    {
        // Make parms accessible.
        _parms = parms;
        
        // Create a queue with fixed capacity of 1 since
        // our channel will have a prefetch limit of 1.
        _deliveryQueue = new ArrayBlockingQueue<>(1);
        
        // Establish our connection to the queue broker.
        // and initialize queues and topics.  There is 
        // some redundancy here since each front-end and
        // each worker initialize all queue artifacts.  
        // Not a problem, but there's room for improvement.
        JobQueueManager.getInstance(JobQueueManager.initParmsFromRuntime());
    }
    
    /* **************************************************************************** */
    /*                               Abstract Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getName:                                                                     */
    /* ---------------------------------------------------------------------------- */
    protected abstract String getName();
    
    /* ---------------------------------------------------------------------------- */
    /* getExchangeType:                                                             */
    /* ---------------------------------------------------------------------------- */
    protected abstract BuiltinExchangeType getExchangeType();
    
    /* ---------------------------------------------------------------------------- */
    /* getExchangeName:                                                             */
    /* ---------------------------------------------------------------------------- */
    protected abstract String getExchangeName();
    
    /* ---------------------------------------------------------------------------- */
    /* getExchangeUse:                                                              */
    /* ---------------------------------------------------------------------------- */
    protected abstract ExchangeUse getExchangeUse();
    
    /* ---------------------------------------------------------------------------- */
    /* getQueueName:                                                                */
    /* ---------------------------------------------------------------------------- */
    protected abstract String getQueueName();
    
    /* ---------------------------------------------------------------------------- */
    /* getBindingKey:                                                               */
    /* ---------------------------------------------------------------------------- */
    protected abstract String getBindingKey();
    
    /* ---------------------------------------------------------------------------- */
    /* process:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Do the real work of processing incoming messages.
     * 
     * @param delivery the message read from the queue
     * @return ack (true) or nack (false)
     */
    protected abstract boolean process(DeliveryResponse delivery);
    
    /* **************************************************************************** */
    /*                               Protected Methods                              */
    /* **************************************************************************** */
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
     * is interrupted or if a fatal runtime error occurs.
     */
    protected void readQueue() 
     throws TapisRuntimeException
    {
      // Initialize the topic and get a channel to it.
      _channel = getChannel();
        
      // Initialize the consumer.
      _consumer = createConsumer();
      
      // Start the consumer and throw exception on error.
      _consumerTag = startConsumer();
      
      // The queue read/job processing loop.
      while (!Thread.currentThread().isInterrupted())
      {
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
          boolean multipleAck = false;
          try {_channel.basicAck(delivery.envelope.getDeliveryTag(), multipleAck);}
            catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_THREAD_ACK_ERROR",
                                           Thread.currentThread().getName(),
                                           Thread.currentThread().getId(),
                                           getName(),
                                           getQueueName(), 
                                           e.getMessage());
              _log.error(msg, e);
            
              // Failures here are fatal.
              String msg2 = MsgUtils.getMsg("JOBS_READER_FATAL_BROKER_ERROR", getName(),
                                            getQueueName(), e.getMessage());
              _log.error(msg2, e);
              throw new TapisRuntimeException(msg2, e);
            }
        }
        else {
          // Reject this unreadable message so that
          // it gets discarded or dead-lettered.
          boolean requeue = false;
          try {_channel.basicReject(delivery.envelope.getDeliveryTag(), requeue);} 
            catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_THREAD_REJECT_ERROR",
                                           Thread.currentThread().getName(),
                                           Thread.currentThread().getId(),
                                           getName(),
                                           getQueueName(), 
                                           e.getMessage());
              _log.error(msg, e);
              
              // Failures here are fatal.
              String msg2 = MsgUtils.getMsg("JOBS_READER_FATAL_BROKER_ERROR", getName(),
                                            getQueueName(), e.getMessage());
              _log.error(msg2, e);
              throw new TapisRuntimeException(msg2, e);
            }
        }
      }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* cancelConsumer:                                                              */
    /* ---------------------------------------------------------------------------- */
    /** Cancel the consumer on our channel.  This method can be called during 
     * shutdown processing. 
     */
    protected void cancelConsumer() 
    {
        // Cancel input from the consumer.
        if (_channel != null)
            try {_channel.basicCancel(_consumerTag);}
            catch (IOException e) {
                String msg = MsgUtils.getMsg("JOBS_QMGR_CANCEL_CONSUMER_ERROR", 
                                             getClass().getSimpleName(),
                                             _channel.getConnection().getId(),
                                             _channel.getChannelNumber());
                _log.error(msg, e);
            }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* makeAlertMessage:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Create a plain text alert message for the alternate and dead letter readers. 
     * 
     * @param queueType either "alternate" or "dead letter"
     * @param envelope the message envelope
     * @param body the body of the message
     * @return the plain text string
     */
    protected String makeAlertMessage(String queueType, Envelope envelope, byte[] body)
    {
        // Print receiver information.
        String s = "A message was received on " + queueType + " queue.\n\n";
        s += "Reader name   : " + getName() + "\n";
        s += "Exchange name : " + getExchangeName() + "\n";
        s += "Exchange type : " + getExchangeType() + "\n";
        s += "Queue name    : " + getQueueName()  + "\n";
        
        s += "Delivery tag  : " + envelope.getDeliveryTag() + "\n";
        s += "Envelope xchg : " + envelope.getExchange()  + "\n";
        s += "Routing key   : " + envelope.getRoutingKey() + "\n";
              
        // Print a limited amount of the body.
        String bodyPrefix;
        if (body == null || body.length < 1) {
            s += "Message length: 0\n";
            bodyPrefix = "** No message body received **";
        }
        else {
            s += "Message length: " + body.length + "\n";
            try {
                // Decode using utf8 for greatest coverage.
                bodyPrefix = new String(body, 0, Math.min(body.length, MAX_BODY_PREFIX_LEN), 
                                        "UTF-8");
            } 
            catch (Exception e) {
                String msg = MsgUtils.getMsg("ALOE_BYTE_ARRAY_DECODE", 
                                             new String(Hex.encodeHex(body)));
                _log.error(msg, e);
                bodyPrefix = "** Unable to decode message body **";
            }
        }
        s += "Message body  :\n\n" + bodyPrefix;
        
        return s;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getStartUpInfo:                                                        */
    /* ---------------------------------------------------------------------- */
    protected String getStartUpInfo(String queueName, String exchangeName)
    {
      // Get the already initialized runtime configuration.
      RuntimeParameters runParms = RuntimeParameters.getInstance();
        
      // Dump the parms.
      StringBuilder buf = new StringBuilder(2500); // capacity to avoid resizing
      buf.append("\n------- Starting ");
      buf.append(_parms.name);
      buf.append(" -------");
      buf.append("\nBinding Key: ");
      buf.append(_parms.bindingKey);
      buf.append("\nQueue Name: ");
      buf.append(queueName);
      buf.append("\nExchange Name: ");
      buf.append(exchangeName);
      
      // Dump the runtime configuration.
      runParms.getRuntimeInfo(buf);
      buf.append("\n---------------------------------------------------\n");
      
      return buf.toString();
    }
    
    /* **************************************************************************** */
    /*                                 Private Methods                              */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getChannel:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Get a new channel, create the exchange, create and bind the queue.  The
     * caller is responsible for closing the returned channel.
     * 
     * @return a new channel 
     * @throws AloeRuntimeException
     */
    private Channel getChannel()
     throws TapisRuntimeException
    {
        // Get the queue manager singleton.
        JobQueueManager qm = JobQueueManager.getInstance();
        
        // Create the inbound channel.
        Channel channel = null;
        try {
          // Create a temporary channel.
          try {channel = qm.getNewInChannel();}
            catch (Exception e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_CHANNEL_ERROR");
              _log.error(msg, e);
              throw e;
            }
        
          // Set the prefetch count so that the consumer using this 
          // channel only receives the next request after the previous
          // request has been acknowledged.
          int prefetchCount = 1;
          try {channel.basicQos(prefetchCount);}
              catch (IOException e) {
                  String msg = MsgUtils.getMsg("JOBS_WORKER_CHANNEL_PREFETCH_ERROR", 
                                  getName(), channel.getChannelNumber(), e.getMessage());
                  _log.error(msg, e);
                  throw new JobQueueException(msg, e);
              }
          
          // Get the tenant exchange name. 
          String exchangeName = getExchangeName();
          
          // Create the exchange.
          final boolean durable = true;
          final boolean autoDelete = false;
          try {channel.exchangeDeclare(exchangeName, getExchangeType().getType(), durable, autoDelete,
                                       qm.getExchangeArgs(getExchangeUse()));}
            catch (IOException e) {
                String msg = MsgUtils.getMsg("JOBS_QMGR_XCHG_ERROR",
                                              qm.getInConnectionName(), channel.getChannelNumber(), 
                                              e.getMessage());
                _log.error(msg, e);
                throw new JobQueueException(msg, e);
            }
          
          // Create the queue with the configured name.
          final boolean exclusive = false;
          String queueName = getQueueName();
          try {channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);}
              catch (IOException e) {
                  String msg = MsgUtils.getMsg("JOBS_QMGR_Q_DECLARE_ERROR", "topic", 
                                               queueName, qm.getInConnectionName(), 
                                               channel.getChannelNumber(), e.getMessage());
                  _log.error(msg, e);
                  throw new JobQueueException(msg, e);
              }
         
          // Bind the queue to the exchange using for each binding key.
          try {
              channel.queueBind(queueName, exchangeName, getBindingKey());
          }
          catch (IOException e) {
              String msg = MsgUtils.getMsg("JOBS_QMGR_Q_BIND_ERROR", "topic", queueName, 
                                           getBindingKey(), qm.getInConnectionName(), 
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
            
            // Failures here are fatal.
            String msg = MsgUtils.getMsg("JOBS_READER_FATAL_BROKER_ERROR", getName(),
                                         getQueueName(), e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
        
        return channel;
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
          
          // Queue the response.
          try {_deliveryQueue.put(delivery);}
            catch (InterruptedException e) {
              String msg = MsgUtils.getMsg("JOBS_THREAD_CONSUMER_INTERRUPTED",
                                            Thread.currentThread().getName(),
                                            Thread.currentThread().getId(),
                                            getName(), 
                                            getQueueName());
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
     * @throws AloeRuntimeException on channel errors.
     */
    private String startConsumer() throws TapisRuntimeException
    {
      // We don't auto-acknowledge topic broadcasts.
      boolean autoack = false;
      String consumerTag = null;
      try {
          // Save the server generated tag for this consumer.  The tag can be used
          // as input on other APIs, such as basicCancel.
          consumerTag = _channel.basicConsume(getQueueName(), autoack, _consumer);
      }
      catch (Exception e) {
        String msg = MsgUtils.getMsg("JOBS_THREAD_CONSUMER_START_ERROR",
                                     Thread.currentThread().getName(),
                                     Thread.currentThread().getId(),
                                     getName(),
                                     getQueueName(), 
                                     e.getMessage());
        _log.error(msg, e);
        throw new TapisRuntimeException(msg, e);
      }
      
      return consumerTag;
    }
}

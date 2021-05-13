package edu.utexas.tacc.tapis.jobs.reader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager.ExchangeUse;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;

/** This class reads the tenant's dead letter queue, records the incident and
 * sends an email to the support email address.  RabbitMQ puts messages on dead
 * letter queues under the following circumstances:
 * 
 *    The message is rejected (basic.reject or basic.nack) with requeue=false,
 *    The TTL for the message expires; or
 *    The queue length limit is exceeded.
 * 
 * This class services all tenants and thus does not use the tenant id 
 * passed in as a parameter.  For convenience, the main method will supply
 * a pseudo-tenant id so that one does not have to be provided on the 
 * command line.
 * 
 * This class does not currently access the database so it can be started 
 * with the environment setting 'aloe.db.connection.pool.size=0'.  This will
 * cause a runtime exception if any attempt is made to connect to the
 * database.
 * 
 * @author rcardone
 */
public final class DeadLetterReader
 extends AbstractQueueReader
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DeadLetterReader.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The tenant's alternate queue name.
    private final String _queueName;
    
    // Name of the exchange used by this queue.
    private final String _exchangeName;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DeadLetterReader(QueueReaderParameters parms) 
    {
        // Assign superclass field.
        super(parms);
        
        // Force use of the default binding key.
        _parms.bindingKey = JobQueueManagerNames.DEFAULT_BINDING_KEY;
        
        // Save the topic name in a field.
        _queueName = JobQueueManagerNames.getDeadLetterQueueName();
        
        // Save the exchange name;
        _exchangeName = JobQueueManagerNames.getDeadLetterExchangeName();
        
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
     throws JobException 
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
        DeadLetterReader reader = new DeadLetterReader(parms);
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
        // Log the unexpected message event.
        String msg = MsgUtils.getMsg("JOBS_QUEUE_DEAD_LETTER_MSG", 
                                     JobQueueManager.getInstance().dumpMessageInfo(
                                        delivery.consumerTag, delivery.envelope, delivery.properties, 
                                        delivery.body));
        _log.error(msg);
        
        // Create the plain text alert message.
        String alertMessage = makeAlertMessage("dead letter", delivery.envelope, delivery.body);
        
        // Can we send an email?
        RuntimeParameters runParms = RuntimeParameters.getInstance();
        if (!StringUtils.isBlank(runParms.getSupportEmail())) 
        {
            // Send an email alert.
            String subject = "Dead letter";
            try {
                // Get the email client based on program configuration.
                EmailClient client = EmailClientFactory.getClient(runParms);
                client.send(runParms.getSupportName(), runParms.getSupportEmail(), 
                            subject, alertMessage, HTMLizer.htmlize(alertMessage));
            } catch (Exception e) {
                String msg1 = MsgUtils.getMsg("JOBS_MAIL_SEND_ERROR", subject, runParms.getSupportEmail());
                _log.error(msg1, e);
            }
        }
        
        return true;
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
    protected ExchangeUse getExchangeUse() {return ExchangeUse.DEAD;}
    
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
}

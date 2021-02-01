package edu.utexas.tacc.tapis.jobs.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParseException;
import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobCancelMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobPauseMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobStatusMsg;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** This is the thread subclass that binds to the tenant command topic with
 * a job-specific binding key.  After initializing its channel, any delivered
 * messages are processed by the superclass which handles all commands from
 * the tenant command topic. 
 * 
 * @author rcardone
 *
 */
final class JobTopicProcessor 
  extends CmdTopicProcessor
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(JobTopicProcessor.class);
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // The specific job for which this processor handles messages. 
  private final Job _job;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  JobTopicProcessor(JobWorker jobWorker, Job job)
  {
    super(jobWorker);
    _job = job;
  }
  
  /* ********************************************************************** */
  /*                            Protected Methods                           */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getNextMessage:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Provide parameter for super class message retrieval method. */
  @Override
  protected void getNextMessage()
  {
      // Initialize channel parameters.
      JobQueueManager qmgr = JobQueueManager.getInstance();
      String jobUuid       = _job.getUuid().toString();
      
      // Generate the topic related names.  Invariant: topicName == JobWorkerThread._qname.  
      String exchangeName = JobQueueManagerNames.getCmdExchangeName();
      String topicName    = JobQueueManagerNames.getCmdSpecificJobTopicName(jobUuid);
      String bindingKey   = JobQueueManagerNames.getCmdSpecificJobBindingKey(jobUuid);

      // Read messages from the tenant topic queue that bind to:
      //
      //    aloe.jobq.cmd.worker.jid.<job-uuid>.#
      //
      String[] bindingKeys = new String[] {bindingKey};
      
      // All error handling is performed by super class.
      NextMessageParms p = new NextMessageParms(exchangeName, 
                                                BuiltinExchangeType.TOPIC, 
                                                topicName, 
                                                bindingKeys);
      
      // Read the queue until the thread terminates.
      getNextJobSpecificMessage(p);
  }

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
    
    // Decode the input.
    String body = new String(delivery.body);
    CmdMsg cmdMsg = null;
    try {cmdMsg = TapisGsonUtils.getGson(true).fromJson(body, CmdMsg.class);}
        catch (Exception e) {
            if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
            String msg = MsgUtils.getMsg("ALOE_JSON_PARSE_ERROR", getProcessorName(), body, e.getMessage());
            _log.error(msg, e);
            return false;
        }
    
    // Make sure we got some message type.
    if (cmdMsg.msgType == null) {
        String msg = MsgUtils.getMsg("JOBS_WORKER_INVALD_MSG_TYPE", "null", getProcessorName());
        _log.error(msg);
        return false;
    }
    
    // Determine the precise command type, populate an object of that type
    // and then call the command-specific processor.
    boolean ack = true;
    try {
        switch (cmdMsg.msgType) {
            case JOB_STATUS:  
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, JobStatusMsg.class));
                break;
            case JOB_CANCEL: 
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, JobCancelMsg.class));
                break;
            case JOB_PAUSE: 
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, JobPauseMsg.class));
                break;
                
            // The binding keys should prevent all other commands from coming through here.
            // If we get here it means that either the sender sent the wrong kind of command
            // here or the binding keys are not filtering out inappropriate messages.
            default:
                ack = processCommand(cmdMsg); // This should not happen.
        }
    }
    catch (JsonParseException e) {
        if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
        String msg = MsgUtils.getMsg("ALOE_JSON_PARSE_ERROR", getProcessorName(), body, e.getMessage());
        _log.error(msg, e);
        ack = false;
    }
    catch (Exception e) {
        if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
        String msg = MsgUtils.getMsg("JOBS_WORKER_MSG_PROCESSING_ERROR", getProcessorName(), 
                                     body, e.getMessage());
        _log.error(msg, e);
        ack = false;
    }

    return ack;
  }
  
  /* ********************************************************************** */
  /*                            Private Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(JobStatusMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
      
      // TODO:  process jobstatus cmd
      
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(JobCancelMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
      
      // Not a lot happening here--just set the field that
      // indicates an asynchronous message was received.
      _job.setCmdMsg(cmd);
      
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(JobPauseMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
      
      // Not a lot happening here--just set the field that
      // indicates an asynchronous message was received.
      _job.setCmdMsg(cmd);
      
      return true;
  }
}

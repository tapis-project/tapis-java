package edu.utexas.tacc.tapis.jobs.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParseException;
import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.WkrResumeMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.WkrShutdownMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.WkrStatusMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.WkrSuspendMsg;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** Processor that runs a worker thread and receives commands on the tenant
 * worker's topic.  The command can target all workers in the tenant or a
 * specific tenant worker.  The JobTopicProcessor handles command that target
 * the worker executing a specific job (if such a worker is running).
 * 
 * @author rcardone
 */
class CmdTopicProcessor 
  extends AbstractProcessor
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(CmdTopicProcessor.class);
  
   /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  CmdTopicProcessor(JobWorker jobWorker){super(jobWorker);}
  
  /* ********************************************************************** */
  /*                           Protected Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getNextMessage:                                                        */
  /* ---------------------------------------------------------------------- */
  /** Provide parameter for super class message retrieval method. */
  @Override
  protected void getNextMessage()
  {
      // Initialize channel parameters.
      String wkrName = _jobWorker.getParms().name;
      
      // Read messages from the tenant topic queue that bind to:
      //
      //    tapis.jobq.cmd.worker
      //    tapis.jobq.cmd.worker.wid.<worker-uuid>.#
      //
      String[] bindingKeys = new String[] 
          {
              JobQueueManagerNames.getCmdAllWorkerBindingKey(), 
              JobQueueManagerNames.getCmdSpecificWorkerBindingKey(_jobWorker.getUUID().toString())
          };
      
      // All error handling is performed by super class. 
      // Invariant: topicName == JobWorkerThread._qname.
      NextMessageParms p = new NextMessageParms(JobQueueManagerNames.getCmdExchangeName(), 
                                                BuiltinExchangeType.TOPIC, 
                                                JobQueueManagerNames.getCmdTopicName(wkrName), 
                                                bindingKeys);
      getNextMessage(p);
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
            case WKR_STATUS:  
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, WkrStatusMsg.class));
                break;
            case WKR_SHUTDOWN: 
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, WkrShutdownMsg.class));
                break;
            case WKR_SUSPEND: 
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, WkrSuspendMsg.class));
                break;
            case WKR_RESUME: 
                ack = processCommand(TapisGsonUtils.getGson(true).fromJson(body, WkrResumeMsg.class));
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
  
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  protected boolean processCommand(CmdMsg cmd)
  {
      // To get here we must of sent a message type that this processor
      // is not expected to handle.
      String msg = MsgUtils.getMsg("JOBS_WORKER_INVALD_MSG_TYPE", cmd.msgType.name(), 
                                   getProcessorName());
      _log.error(msg);
      return false;
  }
  
  /* ********************************************************************** */
  /*                            Private Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(WkrShutdownMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
      
      // Check worker id if one was specified.  If specified and
      // the id doesn't match this worker's id, ignore the command.
      // (Noise in the system.)
      if ((cmd.workerUuid != null) && 
          !cmd.workerUuid.equals(_jobWorker.getUUID().toString())) {
          if (_log.isDebugEnabled())
              _log.debug(MsgUtils.getMsg("JOBS_WORKER_IGNORE_CMD", 
                                         _jobWorker.getParms().name, 
                                         cmd.msgType.name(), cmd.workerUuid));
          return true;
      }
      
      // TODO: implement force=false
      
      // Send the shutdown signal to the main worker thread.
      _jobWorker.shutdown();
      
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(WkrStatusMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
           
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(WkrSuspendMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
      
      // TODO:  process cmd
      
      return true;
  }
  
  /* ---------------------------------------------------------------------- */
  /* processCommand:                                                        */
  /* ---------------------------------------------------------------------- */
  private boolean processCommand(WkrResumeMsg cmd)
  {
      // Trace command.
      if (_log.isDebugEnabled())
          _log.debug(MsgUtils.getMsg("JOBS_WORKER_CMD_RECEIVED", getProcessorName(),
                                     TapisUtils.toString(cmd)));
      
      // TODO:  process cmd
      
      return true;
  }
  
}

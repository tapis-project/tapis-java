package edu.utexas.tacc.tapis.jobs.queue.messages.recover;

import java.util.concurrent.atomic.AtomicLong;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This is the base class for all recovery messages.  The no-args 
 * constructor is provided only for initial message selection when
 * unmarshalling from json.  This class should otherwise be treated
 * as if it was abstract.
 * 
 * @author rcardone
 */
public class RecoverMsg 
{
    // Produce monotonically increasing correlation ids.
    private final static AtomicLong correlationIdGenerator = new AtomicLong();
    
    // Recovery message types.
    public enum RecoverMsgType {RECOVER, 
                                CANCEL_RECOVER,
                                RECOVER_SHUTDOWN}
    
    // Fields
    protected RecoverMsgType  msgType; 
    protected long            correlationId;
    protected String          senderId;
    
    // Constructor
    protected RecoverMsg(RecoverMsgType msgType, String senderId) 
    {
      if (msgType == null) 
        throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "RecoverMsg", "type"));
      
      if (senderId == null) 
          throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "RecoverMsg", "senderId"));
      
      this.msgType = msgType;
      this.senderId = senderId;
      this.correlationId = correlationIdGenerator.incrementAndGet();
    }
    
    // Constructor used to populate a message from json.
    public RecoverMsg(){}

    public RecoverMsgType getMsgType() {
        return msgType;
    }

    public void setMsgType(RecoverMsgType msgType) {
        this.msgType = msgType;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(long correlationId) {
        this.correlationId = correlationId;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }
}

package edu.utexas.tacc.tapis.jobs.queue.messages.event;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This is the base class for all event messages.  The no-args 
 * constructor is provided only for initial message selection when
 * unmarshalling from json.  This class should otherwise be treated
 * as if it was abstract.
 * 
 * @author rcardone
 */
public class EventMsg 
{
  // Every message has a type.
  public enum EventType {
    JOB_SUBMITTED,
    JOB_ACCEPTED,
    WKR_STATUS_RESP
  }
  
  // Fields
  public EventType msgType;
  public String    correlationId;
  public String    senderId;
  
  // Constructor
  protected EventMsg(EventType type) 
  {
    if (type == null) {
      throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "EventMsg", "type"));
    }
    msgType = type;
  }
  
  // Constructor used to populate a message from json.
  public EventMsg(){}
}

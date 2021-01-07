package edu.utexas.tacc.tapis.jobs.queue.messages.cmd;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This is the base class for all command messages.  The no-args 
 * constructor is provided only for initial message selection when
 * unmarshalling from json.  This class should otherwise be treated
 * as if it was abstract.
 * 
 * @author rcardone
 */
public class CmdMsg 
{
  // Every message has a type.
  public enum CmdType {
      WKR_SHUTDOWN,
      WKR_SHUTDOWN_QUIESCE,
      WKR_SUSPEND,
      WKR_RESUME,
      WKR_STATUS,
      JOB_CANCEL,
      JOB_STATUS,
      JOB_PAUSE
  }
  
  // Fields
  public CmdType msgType;
  public String  correlationId;
  public String  senderId;
  
  // Constructor used to create messages
  protected CmdMsg(CmdType type) 
  {
    if (type == null) {
      throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "CmdMsg", "type"));
    }
    msgType = type;
  }
  
  // Constructor used to populate a message from json.
  public CmdMsg(){}
}

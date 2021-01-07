package edu.utexas.tacc.tapis.jobs.queue.messages;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** The base class for all job execution messages posted to user-defined tenant queues
 * or the DefaultQueue.
 * 
 * @author rcardone
 */
public abstract class JobMsg 
{
  // Every message has a type.
  public enum JobMsgType {
    SUBMIT_JOB
  }
  
  // Fields
  public JobMsgType msgType;
  
  // Constructor
  protected JobMsg(JobMsgType type) 
  {
    if (type == null) {
      throw new IllegalArgumentException(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "JobMsg", "type"));
    }
    msgType = type;
  }
}

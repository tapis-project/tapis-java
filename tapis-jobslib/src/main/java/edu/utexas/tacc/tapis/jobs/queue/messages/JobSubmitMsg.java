package edu.utexas.tacc.tapis.jobs.queue.messages;

public final class JobSubmitMsg
 extends JobMsg
{
  // Fields
  public String created;
  public String uuid;
  
  // Constructor
  public JobSubmitMsg() {super(JobMsg.JobMsgType.SUBMIT_JOB);}
}

package edu.utexas.tacc.tapis.jobs.queue.messages;

public final class JobSubmitMsg
 extends JobMsg
{
  // Fields
  private String created;
  private String uuid;
  
  // Constructor
  public JobSubmitMsg() {super(JobMsg.JobMsgType.SUBMIT_JOB);}

  public String getCreated() {return created;}
  public void setCreated(String created) {this.created = created;}
  public String getUuid() {return uuid;}
  public void setUuid(String uuid) {this.uuid = uuid;}
}

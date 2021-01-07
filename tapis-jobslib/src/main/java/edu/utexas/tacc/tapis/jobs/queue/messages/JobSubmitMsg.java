package edu.utexas.tacc.tapis.jobs.queue.messages;

import edu.utexas.tacc.tapis.jobs.model.Job;

public final class JobSubmitMsg
 extends JobMsg
{
  // Fields
  public Job job;
  
  // Constructor
  public JobSubmitMsg() {super(JobMsg.JobMsgType.SUBMIT_JOB);}
}

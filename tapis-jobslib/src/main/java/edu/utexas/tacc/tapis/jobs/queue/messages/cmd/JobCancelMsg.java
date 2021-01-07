package edu.utexas.tacc.tapis.jobs.queue.messages.cmd;

public final class JobCancelMsg 
 extends CmdMsg
{
    public String jobuuid;
    public JobCancelMsg() {super(CmdType.JOB_CANCEL);}
}

package edu.utexas.tacc.tapis.jobs.queue.messages.cmd;

public final class WkrShutdownMsg 
 extends CmdMsg
{
    public boolean force = true;
    public String  tenantId;
    public String  workerUuid;
    
    public WkrShutdownMsg() {super(CmdType.WKR_SHUTDOWN);}
}

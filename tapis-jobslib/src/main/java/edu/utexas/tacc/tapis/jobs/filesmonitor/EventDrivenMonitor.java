package edu.utexas.tacc.tapis.jobs.filesmonitor;

import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public final class EventDrivenMonitor 
 implements TransferMonitor
{
    @Override
    public void monitorTransfer(Job job, String transferId, String corrId) 
     throws TapisException 
    {}

    @Override
    public boolean isAvailable() {return false;}
}

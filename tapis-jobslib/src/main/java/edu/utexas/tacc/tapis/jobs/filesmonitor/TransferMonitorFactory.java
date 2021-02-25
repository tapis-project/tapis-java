package edu.utexas.tacc.tapis.jobs.filesmonitor;

import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class TransferMonitorFactory 
{
    // These values don't change during execution, so they can be cached.
    private static final boolean _eventDrivenAvailable = (new EventDrivenMonitor()).isAvailable();
    private static final boolean _pollingAvailable = (new PollingMonitor()).isAvailable();
    
    /**  Find the monitor with the highest precedence that's available.
     * @return the highest priority available monitor
     * @throws TapisRuntimeException on software misconfiguration
     */
    public static TransferMonitor getMonitor()
     throws TapisRuntimeException
    {
        // Try each monitor in the prefered order.
        if (_eventDrivenAvailable) return new EventDrivenMonitor();
        if (_pollingAvailable)     return new PollingMonitor();
        
        // Houston, we have a compile-time problem.
        String msg = MsgUtils.getMsg("JOBS_NO_TRANSFER_MONITOR");
        throw new TapisRuntimeException(msg);
    }
}

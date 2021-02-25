package edu.utexas.tacc.tapis.jobs.filesmonitor;

import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/** All monitoring of Files service transfers is performed by implementations
 * of this class.
 * 
 * @author rcardone
 */
public interface TransferMonitor 
{
    /** The monitoring command that blocks the calling thread until the transfer 
     * completes successfully or fails with an exception.
     * 
     * @param job the job that initiated the transfer
     * @param the uuid assigned to this task by Files
     * @param corrId the correlation id (or tag) assigned by Jobs and associated with the transfer
     * @throws TapisException when the transfer does not complete successfully
     */
    void monitorTransfer(Job job, String transferId, String corrId) 
      throws TapisException;
    
    /** Determine if a particular monitoring implementation is available.  This
     * method is used by the factory class to determine which type of monitoring
     * will be performed.
     * 
     * @return true if the monitor implementation is available, false otherwise
     */
    boolean isAvailable();
}

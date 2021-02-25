package edu.utexas.tacc.tapis.jobs.filesmonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.ApiException;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTask;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskResponse;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class PollingMonitor
 implements TransferMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(PollingMonitor.class);


    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* monitorByCorrelationId:                                                */
    /* ---------------------------------------------------------------------- */
     /** The monitoring command that blocks the calling thread until the transfer 
     * completes successfully or fails with an exception.
     * 
     * @param job the job that initiated the transfer
     * @param the uuid assigned to this task by Files
     * @param corrId the correlation id (or tag) associated with the transfer
     * @throws TapisException when the transfer does not complete successfully
     */
    @Override
    public void monitorTransfer(Job job, String transferId, String corrId)
     throws TapisException 
    {
        // Get the client from the context.
        var jobCtx = job.getJobCtx(); 
        FilesClient filesClient = jobCtx.getServiceClient(FilesClient.class);

        // Poll the Files service until the transfer completes or fails.
        while (true) {
            // *** Async command check ***
            try {jobCtx.checkCmdMsg();}
            catch (JobAsyncCmdException e) {
                // Cancel the transfer before passing the exception up.
                jobCtx.getJobFileManager().cancelTransfer(transferId);
                throw e;
            }

            // Get the transfer information.
            // TODO: make more resilent
            TransferTaskResponse resp = null;
            try {resp = filesClient.transfers().getTransferTask(transferId);}
                catch (ApiException e) {
                    String msg = MsgUtils.getMsg("JOBS_GET_TRANSFER_ERROR", job.getUuid(),
                                                 transferId, e.getCode(), e.getMessage());
                    throw new TapisImplException(msg, e, e.getCode());
                }
            
            // Check result integrity.
            TransferTask task = resp.getResult();
            if (task == null) {
                String msg = MsgUtils.getMsg("JOBS_INVALID_TRANSFER_RESULT", job.getUuid(), transferId, corrId);
                throw new JobException(msg);
            }
            String status = task.getStatus();
            if (status == null) {
                String msg = MsgUtils.getMsg("JOBS_INVALID_TRANSFER_RESULT", job.getUuid(), transferId, corrId);
                throw new JobException(msg);
            }
            
            // Successful termination, we don't need to poll anymore.
            if (status.equals("COMPLETED")) {
                _log.debug(MsgUtils.getMsg("JOBS_TRANSFER_COMPLETE", job.getUuid(), transferId, corrId));
                break;
            }
            
            // Unsuccessful termination.
            if (status.equals("FAILED") || status.equals("CANCELLED")) {
                String msg = MsgUtils.getMsg("JOBS_TRANSFER_INCOMPLETE", job.getUuid(), transferId, corrId, status);
                throw new JobException(msg);
            }
            
            // Sleep for the prescribed amount of time.
            
        }
    }

    /* ---------------------------------------------------------------------- */
    /* isAvailable:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Fall back to polling the Files service to detect when a transfer is complete.
     * @return true if this implementation can be used, false otherwise
     */
    @Override
    public boolean isAvailable() {return true;}
}

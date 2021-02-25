package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.util.HashSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.ApiException;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskResponse;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao.TransferValueType;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.filesmonitor.TransferMonitorFactory;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class JobFileManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobFileManager.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final JobExecutionContext _jobCtx;
    private final Job                 _job;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobFileManager(JobExecutionContext ctx)
    {
        _jobCtx = ctx;
        _job = ctx.getJob();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* createDirectories:                                                     */
    /* ---------------------------------------------------------------------- */
    public void createDirectories() throws TapisImplException
    {
        // Get the client from the context.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // Get the IO targets for the job.
        var ioTargets = _jobCtx.getJobIOTargets();
        
        // Create a set to that records the directories already created.
        var createdSet = new HashSet<String>();
        
        // ---------------------- Exec System Exec Dir ----------------------
        // Create the directory on the system.
        try {
            filesClient.operations().mkdir(ioTargets.getExecTarget().systemId, 
                                           ioTargets.getExecTarget().dir);
        } catch (ApiException e) {
            String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                         ioTargets.getExecTarget().host,
                                         _job.getOwner(), _job.getTenant(),
                                         ioTargets.getExecTarget().dir);
            throw new TapisImplException(msg, e, e.getCode());
        }
        
        // Save the created directory key to avoid attempts to recreate it.
        createdSet.add(getDirectoryKey(ioTargets.getExecTarget().systemId, 
                                       ioTargets.getExecTarget().dir));
        
        // ---------------------- Exec System Output Dir ----------------- 
        // See if the output dir is the same as the exec dir.
        var execSysOutputDirKey = getDirectoryKey(ioTargets.getOutputTarget().systemId, 
                                                  ioTargets.getOutputTarget().dir);
        if (!createdSet.contains(execSysOutputDirKey)) {
            // Create the directory on the system.
            try {
                filesClient.operations().mkdir(ioTargets.getOutputTarget().systemId, 
                                               _job.getExecSystemOutputDir());
            } catch (ApiException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             ioTargets.getOutputTarget().host,
                                             _job.getOwner(), _job.getTenant(),
                                             ioTargets.getOutputTarget().dir);
                throw new TapisImplException(msg, e, e.getCode());
            }
            
            // Save the created directory key to avoid attempts to recreate it.
            createdSet.add(execSysOutputDirKey);
        }
        
        // ---------------------- Exec System Input Dir ------------------ 
        // See if the input dir is the same as any previously created dir.
        var execSysInputDirKey = getDirectoryKey(ioTargets.getInputTarget().systemId, 
                                                 ioTargets.getInputTarget().dir);
        if (!createdSet.contains(execSysInputDirKey)) {
            // Create the directory on the system.
            try {
                filesClient.operations().mkdir(ioTargets.getInputTarget().systemId, 
                                               ioTargets.getInputTarget().dir);
            } catch (ApiException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             ioTargets.getInputTarget().host,
                                             _job.getOwner(), _job.getTenant(),
                                             ioTargets.getInputTarget().dir);
                throw new TapisImplException(msg, e, e.getCode());
            }
            
            // Save the created directory key to avoid attempts to recreate it.
            createdSet.add(execSysInputDirKey);
        }
        
        // ---------------------- Archive System Dir ---------------------
        // See if the archive dir is the same as any previously created dir.
        var archiveSysDirKey = getDirectoryKey(_job.getArchiveSystemId(), 
                                               _job.getArchiveSystemDir());
        if (!createdSet.contains(archiveSysDirKey)) {
            // Create the directory on the system.
            try {
                filesClient.operations().mkdir(_job.getArchiveSystemId(), 
                                               _job.getArchiveSystemDir());
            } catch (ApiException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             _jobCtx.getArchiveSystem().getHost(),
                                             _job.getOwner(), _job.getTenant(),
                                             _job.getArchiveSystemDir());
                throw new TapisImplException(msg, e, e.getCode());
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* stageInputs:                                                           */
    /* ---------------------------------------------------------------------- */
    public void stageInputs() throws TapisException
    {
        // Get the client from the context.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // -------------------- Assign Transfer Tasks --------------------
        // Get the job input objects.
        var fileInputs = _job.getFileInputsSpec();
        if (fileInputs.isEmpty()) return;
        
        // Create the list of elements to send to files.
        var tasks = new TransferTaskRequest();
        
        // Assign each input task.
        for (var fileInput : fileInputs) {
            // Skip files that are already in-place. 
            if (fileInput.getInPlace() != null && fileInput.getInPlace()) continue;
            
            // Assign the task.
            // TODO: Need to pass required value to Files.
            var task = new TransferTaskRequestElement().
                            sourceURI(fileInput.getSourceUrl()).
                            destinationURI(fileInput.getTargetPath());
            tasks.addElementsItem(task);
        }
        
        // -------------------- Submit Transfer Request ------------------
        // Generate the probabilistically unique tag returned in every event
        // associated with this transfer.
        var tag = UUID.randomUUID().toString();
        tasks.setTag(tag);
        
        // Save the tag now to avoid any race conditions involving asynchronous events.
        // The in-memory job is updated with the tag value.
        _jobCtx.getJobsDao().updateTransferValue(_job, tag, TransferValueType.InputCorrelationId);
        
        // Submit the transfer request.
        TransferTaskResponse resp = null;
        try {resp = filesClient.transfers().createTransferTask(tasks);} 
        catch (ApiException e) {
            String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "input", _job.getUuid(),
                                         e.getCode(), e.getMessage());
            throw new TapisImplException(msg, e, e.getCode());
        }
        
        // Get the transfer id.
        String transferId = null;
        var transferTask = resp.getResult();
        if (transferTask != null) {
            var uuid = transferTask.getUuid();
            if (uuid != null) transferId = uuid.toString();
        }
        if (transferId == null) {
            String msg = MsgUtils.getMsg("JOBS_NO_TRANSFER_ID", "input", _job.getUuid());
            throw new JobException(msg);
        }
        
        // Save the transfer id and update the in-memory job with the transfer id.
        _jobCtx.getJobsDao().updateTransferValue(_job, transferId, TransferValueType.InputTransferId);
        
        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, tag);
    }
    
    /* ---------------------------------------------------------------------- */
    /* cancelTransfer:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Best effort attempt to cancel a transfer.
     * 
     * @param transferId the transfer's uuid
     */
    public void cancelTransfer(String transferId)
    {
        // Get the client from the context.
        FilesClient filesClient = null;
        try {filesClient = _jobCtx.getServiceClient(FilesClient.class);}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                return;
            }
        
        // Issue the cancel command.
        try {filesClient.transfers().cancelTransferTask(transferId);}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
            }
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDirectoryKey:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Create a hash key for a system/directory combination.
     * 
     * @param systemId the system id
     * @param directory the directory path
     * @return a string to use as a hash key to identify the system/directory
     */
    private String getDirectoryKey(String systemId, String directory)
    {
        return systemId + "|" + directory;
    }
}

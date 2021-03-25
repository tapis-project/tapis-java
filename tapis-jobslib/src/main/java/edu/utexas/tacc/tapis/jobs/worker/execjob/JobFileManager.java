package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.util.HashSet;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTask;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao.TransferValueType;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.filesmonitor.TransferMonitorFactory;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.InputSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobFileManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobFileManager.class);
    
    // Special transfer id value indicating no files to stage.
    private static final String NO_FILE_INPUTS = "no inputs";

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
    /** Create the directories used for I/O on this job.  The directories may
     * already exist
     * 
     * @throws TapisImplException
     * @throws TapisServiceConnectionException
     */
    public void createDirectories() 
     throws TapisImplException, TapisServiceConnectionException
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
            filesClient.mkdir(ioTargets.getExecTarget().systemId, 
                              ioTargets.getExecTarget().dir);
        } catch (TapisClientException e) {
            String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                         ioTargets.getExecTarget().host,
                                         _job.getOwner(), _job.getTenant(),
                                         ioTargets.getExecTarget().dir, e.getCode());
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
                filesClient.mkdir(ioTargets.getOutputTarget().systemId, 
                                  _job.getExecSystemOutputDir());
            } catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             ioTargets.getOutputTarget().host,
                                             _job.getOwner(), _job.getTenant(),
                                             ioTargets.getOutputTarget().dir, e.getCode());
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
                filesClient.mkdir(ioTargets.getInputTarget().systemId, 
                                 ioTargets.getInputTarget().dir);
            } catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             ioTargets.getInputTarget().host,
                                             _job.getOwner(), _job.getTenant(),
                                             ioTargets.getInputTarget().dir, e.getCode());
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
                filesClient.mkdir(_job.getArchiveSystemId(), 
                                  _job.getArchiveSystemDir());
            } catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             _jobCtx.getArchiveSystem().getHost(),
                                             _job.getOwner(), _job.getTenant(),
                                             _job.getArchiveSystemDir(), e.getCode());
                throw new TapisImplException(msg, e, e.getCode());
            }
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* stageInputs:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Perform or restart the input file staging process.  Both recoverable
     * and non-recoverable exceptions can be thrown from here.
     * 
     * @throws TapisException on error
     */
    public void stageInputs() throws TapisException
    {
        // Determine if we are restarting a previous staging request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.inputTransactionId;
        String corrId     = transferInfo.inputCorrelationId;
        
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already 
        // submitted its transfer request and we are now in recovery processing.  
        // There's no need to resubmit the transfer request in this case.  
        // 
        // It's possible that the corrId was set but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = stageNewInputs(corrId);
        }
        
        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId);
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
        try {filesClient.cancelTransferTask(transferId);}
            catch (Exception e) {_log.error(e.getMessage(), e);}
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
    
    /* ---------------------------------------------------------------------- */
    /* stageNewInputs:                                                        */
    /* ---------------------------------------------------------------------- */
    private String stageNewInputs(String tag) throws TapisException
    {
        // Get the client from the context.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // -------------------- Assign Transfer Tasks --------------------
        // Get the job input objects.
        var fileInputs = _job.getFileInputsSpec();
        if (fileInputs.isEmpty()) return NO_FILE_INPUTS;
        
        // Create the list of elements to send to files.
        var tasks = new TransferTaskRequest();
        
        // Assign each input task.
        for (var fileInput : fileInputs) {
            // Skip files that are already in-place. 
            if (fileInput.getInPlace() != null && fileInput.getInPlace()) continue;
            
            // Determine the optional value.
            Boolean optional = Boolean.FALSE;
            if (fileInput.getMeta() != null && 
                fileInput.getMeta().getRequired() == Boolean.FALSE)
                optional = Boolean.TRUE;
            
            // Assign the task.
            var task = new TransferTaskRequestElement().
                            sourceURI(fileInput.getSourceUrl()).
                            destinationURI(makeExecSysInputPath(fileInput));
            task.setOptional(optional);;
            tasks.addElementsItem(task);
        }
        
        // -------------------- Submit Transfer Request ------------------
        // Note that failures can occur between the two database calls leaving
        // the job record with the correlation id set but not the transfer id.
        // On recovery, a new correlation id will be issued.
        
        // Generate the probabilistically unique tag returned in every event
        // associated with this transfer.
        tasks.setTag(tag);
        
        // Save the tag now to avoid any race conditions involving asynchronous events.
        // The in-memory job is updated with the tag value.
        _jobCtx.getJobsDao().updateTransferValue(_job, tag, TransferValueType.InputCorrelationId);
        
        // Submit the transfer request and get the new transfer id.
        String transferId = createTransferTask(filesClient, tasks);
        
        // Save the transfer id and update the in-memory job with the transfer id.
        _jobCtx.getJobsDao().updateTransferValue(_job, transferId, TransferValueType.InputTransferId);
        
        // Return the transfer id.
        return transferId;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createTransferTask:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Issue a transfer request to Files and return the transfer id.  An
     * exception is thrown if the new transfer id is not attained.
     * 
     * @param tasks the tasks 
     * @return the new, non-null transfer id generated by Files
     * @throws TapisImplException 
     */
    private String createTransferTask(FilesClient filesClient, TransferTaskRequest tasks) 
     throws TapisException
    {
        // Submit the transfer request.
        TransferTask task = null;
        try {task = filesClient.createTransferTask(tasks);} 
        catch (Exception e) {
            // Look for a recoverable error in the exception chain. Recoverable
            // exceptions are those that might indicate a transient network
            // or server error, typically involving loss of connectivity.
            Throwable transferException = 
                TapisUtils.findFirstMatchingException(e, TapisConstants.CONNECTION_EXCEPTION_PREFIX);
            if (transferException != null) {
                throw new TapisServiceConnectionException(transferException.getMessage(), 
                            e, RecoveryUtils.captureServiceConnectionState(
                               filesClient.getBasePath(), TapisConstants.FILES_SERVICE));
            }
            
            // Unrecoverable error.
            if (e instanceof TapisClientException) {
                var e1 = (TapisClientException) e;
                String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "input", _job.getUuid(),
                                             e1.getCode(), e1.getMessage());
                throw new TapisImplException(msg, e1, e1.getCode());
            } else {
                String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "input", _job.getUuid(),
                                             0, e.getMessage());
                throw new TapisImplException(msg, e, 0);
            }
        }
        
        // Get the transfer id.
        String transferId = null;
        if (task != null) {
            var uuid = task.getUuid();
            if (uuid != null) transferId = uuid.toString();
        }
        if (transferId == null) {
            String msg = MsgUtils.getMsg("JOBS_NO_TRANSFER_ID", "input", _job.getUuid());
            throw new JobException(msg);
        }
        
        return transferId;
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeExecSysInputPath:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on the input spec's destination path and the
     * execution system id.  Implicit in the tapis protocol is that the Files
     * service will prefix path portion of the url with  the execution system's 
     * rootDir when actually transferring files. 
     * 
     * The target is never null or empty.
     * 
     * @param fileInput a file input spec
     * @return the tapis url indicating a path on the exec system.
     */
    private String makeExecSysInputPath(InputSpec fileInput)
    {
        // Start with the system id.
        String url = "tapis://" + _job.getExecSystemId();
        
        // Add the job's put input path.
        var inputPath = _job.getExecSystemInputDir();
        if (inputPath.startsWith("/")) url += inputPath;
          else url += "/" + inputPath;
        
        // Add the suffix.
        String dest = fileInput.getTargetPath();
        if (url.endsWith("/") && dest.startsWith("/")) url += dest.substring(1);
        else if (!url.endsWith("/") && !dest.startsWith("/")) url += "/" + dest;
        else url += dest;
        return url;
    }
}

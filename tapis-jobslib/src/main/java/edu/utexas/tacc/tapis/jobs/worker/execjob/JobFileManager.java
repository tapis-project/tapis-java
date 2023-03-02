package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTask;
import edu.utexas.tacc.tapis.files.client.gen.model.ReqTransfer;
import edu.utexas.tacc.tapis.files.client.gen.model.ReqTransferElement;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao.TransferValueType;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.filesmonitor.TransferMonitorFactory;
import edu.utexas.tacc.tapis.jobs.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.uri.TapisLocalUrl;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;
import edu.utexas.tacc.tapis.shared.utils.FilesListSubtree;
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
    
    // Filters are interpretted as globs unless they have this prefix.
    public static final String REGEX_FILTER_PREFIX = "REGEX:";
    
    // Various useful posix permission settings.
    public static final List<PosixFilePermission> RWRW   = SSHScpClient.RWRW_PERMS;
    public static final List<PosixFilePermission> RWXRWX = SSHScpClient.RWXRWX_PERMS;
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // We transfer files in these phases of job processing.
    private enum JobTransferPhase {INPUT, ARCHIVE}
    
    // Archive filter types.
    private enum FilterType {INCLUDES, EXCLUDES}
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final JobExecutionContext _jobCtx;
    private final Job                 _job;
    
    // Unpack shared context directory settings
    private final String             _shareExecSystemExecDir;
    private final String             _shareExecSystemOutputDir;
    private final String             _shareArchiveSystemDir;
    
    // Derived path prefix value removed before filtering.
    private String                    _filterIgnorePrefix;
    
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
        
        _shareExecSystemExecDir   = ctx.getJobSharedAppCtx().getSharingExecSystemExecDirAppOwner();
        _shareExecSystemOutputDir = ctx.getJobSharedAppCtx().getSharingExecSystemOutputDirAppOwner();
        _shareArchiveSystemDir    = ctx.getJobSharedAppCtx().getSharingArchiveSystemDirAppOwner();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* createDirectories:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Create the directories used for I/O on this job.  The directories may
     * already exist.
     * 
     * @throws TapisImplException
     * @throws TapisServiceConnectionException
     */
    public void createDirectories() 
     throws TapisException, TapisServiceConnectionException
    {
        // Get the client from the context.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // Get the IO targets for the job and check that the systems are enabled.
        var ioTargets = _jobCtx.getJobIOTargets();
        
        // Create a set to that records the directories already created.
        var createdSet = new HashSet<String>();
        
        // ---------------------- Exec System Exec Dir ----------------------
        // Create the directory on the system.
        try {
            var sharedAppCtx = _jobCtx.getJobSharedAppCtx().getSharingExecSystemExecDirAppOwner();
            
            filesClient.mkdir(ioTargets.getExecTarget().systemId, 
                              ioTargets.getExecTarget().dir, sharedAppCtx);
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
                var sharedAppCtx = _jobCtx.getJobSharedAppCtx().getSharingExecSystemOutputDirAppOwner();
                filesClient.mkdir(ioTargets.getOutputTarget().systemId, 
                                  _job.getExecSystemOutputDir(), sharedAppCtx);
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
                var sharedAppCtx = _jobCtx.getJobSharedAppCtx().getSharingExecSystemInputDirAppOwner();
                filesClient.mkdir(ioTargets.getInputTarget().systemId, 
                                 ioTargets.getInputTarget().dir, sharedAppCtx);
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
                var sharedAppCtx = _jobCtx.getJobSharedAppCtx().getSharingArchiveSystemDirAppOwner();
                filesClient.mkdir(_job.getArchiveSystemId(), 
                                  _job.getArchiveSystemDir(), sharedAppCtx);
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
        
        // Is there anything to transfer?
        if (transferId.equals(NO_FILE_INPUTS)) return;
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(), 
                                  _job.getStatus().name(), transferId, corrId));
        
        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId);
    }
    
    /* ---------------------------------------------------------------------- */
    /* archiveOutputs:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Perform or restart the output file archiving process.  Both recoverable
     * and non-recoverable exceptions can be thrown from here.
     * 
     * @throws TapisException on error
     * @throws TapisClientException 
     */
    public void archiveOutputs() throws TapisException, TapisClientException
    {
        // Determine if archiving is necessary.
        if (_job.getRemoteOutcome() == JobRemoteOutcome.FAILED_SKIP_ARCHIVE) return;
        
        // Determine if we are restarting a previous staging request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.archiveTransactionId;
        String corrId     = transferInfo.archiveCorrelationId;
        
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already 
        // submitted its transfer request and we are now in recovery processing.  
        // There's no need to resubmit the transfer request in this case.  
        // 
        // It's possible that the corrId was set but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = archiveNewOutputs(corrId);
        }
        
        // Is there anything to transfer?
        if (transferId.equals(NO_FILE_INPUTS)) return;
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(), 
                                  _job.getStatus().name(), transferId, corrId));

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
    
    /* ---------------------------------------------------------------------- */
    /* installExecFile:                                                       */
    /* ---------------------------------------------------------------------- */
    public void installExecFile(String content, String fileName, 
                                List<PosixFilePermission> mod) 
      throws TapisException
    {
        // Calculate the destination file path.
        String destPath = makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                                   _job.getExecSystemExecDir(),
                                   fileName);
        
        // Transfer the wrapper script.
        try {
            // Initialize a scp client.
            var scpClient = _jobCtx.getExecSystemTapisSSH().getScpClient();
            scpClient.uploadBytesToFile(content.getBytes(), destPath, mod, null);
        } 
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SFTP_CMD_ERROR", 
                                         _jobCtx.getExecutionSystem().getId(),
                                         _jobCtx.getExecutionSystem().getHost(),
                                         _jobCtx.getExecutionSystem().getEffectiveUserId(),
                                         _jobCtx.getExecutionSystem().getTenant(),
                                         _job.getUuid(),
                                         destPath, e.getMessage());
            throw new JobException(msg, e);
        } 
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysInputPath:                                               */
    /* ---------------------------------------------------------------------- */
    /** Make the absolute path on the exec system starting at the rootDir, 
     * including the input directory and ending with 0 or more other segments.
     * 
     * @param more 0 or more path segments
     * @return the absolute path
     * @throws TapisException 
     */
    public String makeAbsExecSysInputPath(String... more) 
     throws TapisException
    {
        String[] components = new String[1 + more.length];
        components[0] = _job.getExecSystemInputDir();
        for (int i = 0; i < more.length; i++) components[i+1] = more[i];
        return makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                        components);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysExecPath:                                                */
    /* ---------------------------------------------------------------------- */
    /** Make the absolute path on the exec system starting at the rootDir, 
     * including the exec directory and ending with 0 or more other segments.
     * 
     * @param more 0 or more path segments
     * @return the absolute path
     * @throws TapisException 
     */
    public String makeAbsExecSysExecPath(String... more) 
     throws TapisException
    {
        String[] components = new String[1 + more.length];
        components[0] = _job.getExecSystemExecDir();
        for (int i = 0; i < more.length; i++) components[i+1] = more[i];
        return makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                        components);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysOutputPath:                                              */
    /* ---------------------------------------------------------------------- */
    /** Make the absolute path on the exec system starting at the rootDir, 
     * including the output directory and ending with 0 or more other segments.
     * 
     * @param more 0 or more path segments
     * @return the absolute path
     * @throws TapisException 
     */
    public String makeAbsExecSysOutputPath(String... more) 
     throws TapisException
    {
        String[] components = new String[1 + more.length];
        components[0] = _job.getExecSystemOutputDir();
        for (int i = 0; i < more.length; i++) components[i+1] = more[i];
        return makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                        components);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysTapisLocalPath:                                          */
    /* ---------------------------------------------------------------------- */
    /** Construct the absolute path on the execution system where the pre-positioned
     * tapislocal input resides.
     * 
     * @param execSystemRootDir the root directory of the execution system
     * @param sourceUrl the path under the root directory where the input resides
     * @return the absolute path on the execution system where the input resides 
     */
    public String makeAbsExecSysTapisLocalPath(String execSystemRootDir, 
                                               String sourceUrl)
    {
        return makePath(execSystemRootDir, TapisUtils.extractFilename(sourceUrl));
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
        // -------------------- Assign Transfer Tasks --------------------
        // Get the job input objects.
        var fileInputs = _job.getFileInputsSpec();
        if (fileInputs.isEmpty()) return NO_FILE_INPUTS;
        
        // Create the list of elements to send to files.
        var tasks = new ReqTransfer();
        
        // Assign each input task.
        for (var fileInput : fileInputs) {
            // Skip files that are already in-place. 
            if (fileInput.getSourceUrl().startsWith(TapisLocalUrl.TAPISLOCAL_PROTOCOL_PREFIX))
                continue;
            
            // Assign the task.  Input files have already been assigned their
            // sharing attributes during submission.  For details, see
            // SubmitContext.calculateDirectorySharing().
            var task = new ReqTransferElement().
                            sourceURI(fileInput.getSourceUrl()).
                            destinationURI(makeExecSysInputUrl(fileInput));
            task.setOptional(fileInput.isOptional());
            task.setSrcSharedCtx(fileInput.isSrcSharedAppCtx());
            task.setDestSharedCtx(fileInput.isDestSharedAppCtx());
            tasks.addElementsItem(task);
        }
        
        // Return the transfer id.
        if (tasks.getElements().isEmpty()) return NO_FILE_INPUTS;
        return submitTransferTask(tasks, tag, JobTransferPhase.INPUT);
    }
    
    /* ---------------------------------------------------------------------- */
    /* archiveNewOutputs:                                                     */
    /* ---------------------------------------------------------------------- */
    private String archiveNewOutputs(String tag) 
     throws TapisException, TapisClientException
    {
        // -------------------- Assess Work ------------------------------
        // Get the archive filter spec in canonical form.
        var parmSet = _job.getParameterSetModel();
        var archiveFilter = parmSet.getArchiveFilter();
        if (archiveFilter == null) archiveFilter = new IncludeExcludeFilter();
        archiveFilter.initAll();
        var includes = archiveFilter.getIncludes();
        var excludes = archiveFilter.getExcludes();
        
        // Determine if the archive directory is the same as the output
        // directory on the same system.  If so, we won't apply either of
        // the two filters.
        boolean archiveSameAsOutput = _job.isArchiveSameAsOutput();
        
        // See if there's any work to do at all.
        if (archiveSameAsOutput && !archiveFilter.getIncludeLaunchFiles()) 
            return NO_FILE_INPUTS;
        
        // -------------------- Assign Transfer Tasks --------------------
        // Create the list of elements to send to files.
        var tasks = new ReqTransfer();
        
        // Add the tapis generated files to the task.
        if (archiveFilter.getIncludeLaunchFiles()) addLaunchFiles(tasks);
        
        // There's nothing to do if the archive and output directories are 
        // the same or if we have to exclude all output files. 
        if (!archiveSameAsOutput && !matchesAll(excludes)) {
            // Will any filtering be necessary at all?
            if (excludes.isEmpty() && (includes.isEmpty() || matchesAll(includes))) 
            {
                // We only need to specify the whole output directory  
                // subtree to archive all files.
                var task = new ReqTransferElement().
                        sourceURI(makeExecSysOutputUrl("")).
                        destinationURI(makeArchiveSysUrl(""));
                task.setSrcSharedCtx(_shareExecSystemOutputDir);
                task.setDestSharedCtx(_shareArchiveSystemDir);
                tasks.addElementsItem(task);
            } 
            else 
            {
                // We need to filter each and every file, so we need to retrieve 
                // the output directory file listing.  Get the client from the 
                // context now to catch errors early.  We also set the 
                FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
                var listSubtree = new FilesListSubtree(filesClient, _job.getExecSystemId(), 
                                                       _job.getExecSystemOutputDir());
                listSubtree.setSharedAppCtx(_shareArchiveSystemDir);
                var fileList = listSubtree.list();
                
                // Apply the excludes list first since it has precedence, then
                // the includes list.  The fileList can be modified in both calls.
                applyArchiveFilters(excludes, fileList, FilterType.EXCLUDES);
                applyArchiveFilters(includes, fileList, FilterType.INCLUDES);
                
                // Create a task entry for each of the filtered output files.
                addOutputFiles(tasks, fileList);
            }
        }
        
        // Return a transfer id if tasks is not empty.
        if (tasks.getElements().isEmpty()) return NO_FILE_INPUTS;
        return submitTransferTask(tasks, tag, JobTransferPhase.ARCHIVE);
    }
    
    /* ---------------------------------------------------------------------- */
    /* submitTransferTask:                                                    */
    /* ---------------------------------------------------------------------- */
    private String submitTransferTask(ReqTransfer tasks, String tag,
                                      JobTransferPhase phase)
     throws TapisException
    {
        // Note that failures can occur between the two database calls leaving
        // the job record with the correlation id set but not the transfer id.
        // On recovery, a new correlation id will be issued.
        
        // Get the client from the context now to catch errors early.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // Database assignment keys.
        TransferValueType tid;
        TransferValueType corrId;
        if (phase == JobTransferPhase.INPUT) {
            tid = TransferValueType.InputTransferId;
            corrId = TransferValueType.InputCorrelationId;
        } else {
            tid = TransferValueType.ArchiveTransferId;
            corrId = TransferValueType.ArchiveCorrelationId;
        }
        
        // Generate the probabilistically unique tag returned in every event
        // associated with this transfer.
        tasks.setTag(tag);
        
        // Save the tag now to avoid any race conditions involving asynchronous events.
        // The in-memory job is updated with the tag value.
        _jobCtx.getJobsDao().updateTransferValue(_job, tag, corrId);
        
        // Submit the transfer request and get the new transfer id.
        String transferId = createTransferTask(filesClient, tasks);
        
        // Save the transfer id and update the in-memory job with the transfer id.
        _jobCtx.getJobsDao().updateTransferValue(_job, transferId, tid);
        
        // Return the transfer id.
        return transferId;
    }
    
    /* ---------------------------------------------------------------------- */
    /* addLaunchFiles:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Add task entries to copy the generated tapis launch files to the archive
     * directory.
     * 
     * @param tasks the task collection into which new transfer tasks are inserted
     */
    private void addLaunchFiles(ReqTransfer tasks)
    {
        // There's nothing to do if the exec and archive 
        // directories are same and on the same system.
        if (_job.isArchiveSameAsExec()) return;
        
        // Assign the tasks for the two generated files.
        var task = new ReqTransferElement().
                        sourceURI(makeExecSysExecUrl(JobExecutionUtils.JOB_WRAPPER_SCRIPT)).
                        destinationURI(makeArchiveSysUrl(JobExecutionUtils.JOB_WRAPPER_SCRIPT));
        task.setSrcSharedCtx(_shareExecSystemExecDir);
        task.setDestSharedCtx(_shareArchiveSystemDir);
        tasks.addElementsItem(task);
        if (_jobCtx.usesEnvFile()) {
            task = new ReqTransferElement().
                        sourceURI(makeExecSysExecUrl(JobExecutionUtils.JOB_ENV_FILE)).
                        destinationURI(makeArchiveSysUrl(JobExecutionUtils.JOB_ENV_FILE));
            task.setSrcSharedCtx(_shareExecSystemExecDir);
            task.setDestSharedCtx(_shareArchiveSystemDir);
            tasks.addElementsItem(task);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* addOutputFiles:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Add each output file in list to the archive tasks. 
     * 
     * @param tasks the archive tasks
     * @param fileList the filtered list of files in the job's output directory
     */
    private void addOutputFiles(ReqTransfer tasks, List<FileInfo> fileList)
    {
        // Add each output file as a task element.
        for (var f : fileList) {
            var relativePath = getOutputRelativePath(f.getPath());
            var task = new ReqTransferElement().
                    sourceURI(makeExecSysOutputUrl(relativePath)).
                    destinationURI(makeArchiveSysUrl(relativePath));
            task.setSrcSharedCtx(_shareExecSystemOutputDir);
            task.setDestSharedCtx(_shareArchiveSystemDir);
            tasks.addElementsItem(task);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* matchesAll:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Determine if the filter list will match any string.  Only the most 
     * common way of specifying a pattern that matches all strings are tested. 
     * In addition, combinations of filters whose effect would be to match all
     * strings are not considered.  Simplistic as it may be, filters specified
     * in a reasonable, straightforward manner to match all strings are identified.   
     * 
     * @param filters the list of glob or regex filters
     * @return true if list contains a filter that will match all strings, false 
     *              if no single filter will match all strings
     */
    private boolean matchesAll(List<String> filters)
    {
        // Check the most common ways to express all strings using glob.
        if (filters.contains("**/*")) return true;
        
        // Check the common way to expess all strings using a regex.
        if (filters.contains("REGEX(.*)")) return true;
        
        // No no-op filters found.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* applyArchiveFilters:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Apply either the includes or excludes list to the file list.  In either
     * case, the file list can be modified by having items deleted.
     * 
     * The filter items can either be in glob or regex format.  Each item is 
     * applied to the path of a file info object.  When a match occurs the 
     * appropriate action is taken based on the filter type being processed.
     * 
     * @param filterList the includes or excludes list as identified by the filterType 
     * @param fileList the file list that may have items deleted
     * @param filterType filter indicator
     */
    private void applyArchiveFilters(List<String> filterList, List<FileInfo> fileList, 
                                     FilterType filterType)
    {
        // Is there any work to do?
        if (filterType == FilterType.EXCLUDES) {
            if (filterList.isEmpty()) return;
        } else 
            if (filterList.isEmpty() || matchesAll(filterList)) return;
        
        // Local cache of compiled regexes.  The keys are the filters
        // exactly as defined by users and the values are the compiled 
        // form of those filters.
        HashMap<String,Pattern> regexes   = new HashMap<>();
        HashMap<String,PathMatcher> globs = new HashMap<>();
        
        // Iterate through the file list.
        final int lastFilterIndex = filterList.size() - 1;
        var fileIt = fileList.listIterator();
        while (fileIt.hasNext()) {
            var fileInfo = fileIt.next();
            var path = getOutputRelativePath(fileInfo.getPath());
            for (int i = 0; i < filterList.size(); i++) {
                // Get the current filter.
                String filter = filterList.get(i);
                
                // Use cached filters to match paths.
                boolean matches = matchFilter(filter, path, globs, regexes);
                
                // Removal depends on matches and the filter type.
                if (filterType == FilterType.EXCLUDES) {
                    if (matches) fileIt.remove();
                    break;
                } else {
                    // Remove item only after all include filters have failed to match.
                    if (matches) break; // keep in list 
                    if (!matches && (i == lastFilterIndex)) fileIt.remove();
                }
            }
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getOutputRelativePath:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Strip the job output directory prefix from the absolute pathname before
     * performing a filter matching operation.
     * 
     * @param absPath the absolute path name of a file rooted in the job output directory 
     * @return the path name relative to the job output directory
     */
    private String getOutputRelativePath(String absPath)
    {
        var prefix = getOutputPathPrefix();
        if (absPath.startsWith(prefix))
            return absPath.substring(prefix.length());
        // Special case if Files strips leading slash from output.
        if (!absPath.startsWith("/") && prefix.startsWith("/") &&
            absPath.startsWith(prefix.substring(1)))
            return absPath.substring(prefix.length()-1);
        return absPath;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getOutputPathPrefix:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Assign the filter ignore prefix value for this job.  This value is the
     * path prefix (with trailing slash) that will be removed from all output
     * file path before filtering is carried out.  Users provide glob or regex
     * pattern that are applied to file paths relative to the job output directory. 
     * 
     * @return the prefix to be removed from all path before filter matching
     */
    private String getOutputPathPrefix()
    {
        // Assign the filter ignore prefix the job output directory including 
        // a trailing slash.
        if (_filterIgnorePrefix == null) {
            _filterIgnorePrefix = _job.getExecSystemOutputDir();
            if (!_filterIgnorePrefix.endsWith("/")) _filterIgnorePrefix += "/";
        }
        return _filterIgnorePrefix;
    }
    
    /* ---------------------------------------------------------------------- */
    /* matchFilter:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Determine if the path matches the filter, which can be either a glob
     * or regex.  In each case, the appropriate cache is consulted and, if
     * necessary, updated so that each filter is only compiled once per call
     * to applyArchiveFilters().
     * 
     * @param filter the glob or regex
     * @param path the path to be matched
     * @param globs the glob cache
     * @param regexes the regex cache
     * @return true if the path matches the filter, false otherwise
     */
    private boolean matchFilter(String filter, String path, 
                                HashMap<String,PathMatcher> globs,
                                HashMap<String,Pattern> regexes)
    {
        // Check the cache for glob and regex filters.
        if (filter.startsWith(REGEX_FILTER_PREFIX)) {
            Pattern p = regexes.get(filter);
            if (p == null) {
                p = Pattern.compile(filter.substring(REGEX_FILTER_PREFIX.length()));
                regexes.put(filter, p);
            }
            var m = p.matcher(path);
            return m.matches();
        } else {
            PathMatcher m = globs.get(filter);
            if (m == null) {
                m = FileSystems.getDefault().getPathMatcher("glob:"+filter);
                globs.put(filter, m);
            }
            var pathObj = Paths.get(path);
            return m.matches(pathObj);
        }
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
    private String createTransferTask(FilesClient filesClient, ReqTransfer tasks)
     throws TapisException
    {
        // Tracing.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("FILES_TRANSFER_TASK_REQ", printTasks(tasks)));
        
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
    /* makeExecSysInputUrl:                                                   */
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
    private String makeExecSysInputUrl(JobFileInput fileInput)
    {
        return makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemInputDir(), 
                              fileInput.getTargetPath());
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeExecSysOutputUrl:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on a file pathname and the execution system id.  
     * Implicit in the tapis protocol is that the Files service will prefix path 
     * portion of the url with  the execution system's rootDir when actually 
     * transferring files. 
     * 
     * The pathName can be null or empty.
     * 
     * @param pathName a file path name
     * @return the tapis url indicating a path on the exec system.
     */
    private String makeExecSysExecUrl(String pathName)
    {
        return makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemExecDir(), pathName);
    }

    /* ---------------------------------------------------------------------- */
    /* makeExecSysOutputUrl:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on a file pathname and the execution system id.  
     * Implicit in the tapis protocol is that the Files service will prefix path 
     * portion of the url with  the execution system's rootDir when actually 
     * transferring files. 
     * 
     * The pathName can be null or empty.
     * 
     * @param pathName a file path name
     * @return the tapis url indicating a path on the exec system.
     */
    private String makeExecSysOutputUrl(String pathName)
    {
        return makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemOutputDir(), pathName);
    }

    /* ---------------------------------------------------------------------- */
    /* makeArchiveSysUrl:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on a file pathname and the archive system id.  
     * Implicit in the tapis protocol is that the Files service will prefix path 
     * portion of the url with  the execution system's rootDir when actually 
     * transferring files. 
     * 
     * The pathName can be null or empty.
     * 
     * @param pathName a file path name
     * @return the tapis url indicating a path on the archive system.
     */
    private String makeArchiveSysUrl(String pathName)
    {
        return makeSystemUrl(_job.getArchiveSystemId(), _job.getArchiveSystemDir(), pathName);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeSystemUrl:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on the systemId, a base path on thate system
     * and a file pathname.  
     *   
     * Implicit in the tapis protocol is that the Files service will prefix path 
     * portion of the url with  the execution system's rootDir when actually 
     * transferring files.
     * 
     * The pathName can be null or empty.
     * 
     * @param systemId the target tapis system
     * @param basePath the jobs base path (input, output, exec) relative to the system's rootDir
     * @param pathName the file pathname relative to the basePath
     * @return the tapis url indicating a path on the exec system.
     */
    private String makeSystemUrl(String systemId, String basePath, String pathName)
    {
        // Start with the system id.
        String url = TapisUrl.TAPIS_PROTOCOL_PREFIX + systemId;
        
        // Add the job's put input path.
        if (basePath.startsWith("/")) url += basePath;
          else url += "/" + basePath;
        
        // Add the suffix.
        if (StringUtils.isBlank(pathName)) return url;
        if (url.endsWith("/") && pathName.startsWith("/")) url += pathName.substring(1);
        else if (!url.endsWith("/") && !pathName.startsWith("/")) url += "/" + pathName;
        else url += pathName;
        return url;
    }

    /* ---------------------------------------------------------------------- */
    /* makePath:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Make a path from components with proper treatment of slashes.
     * 
     * @param first non-null start of path
     * @param more 0 or more additional segments
     * @return the path as a string
     */
    private String makePath(String first, String... more)
    {
        return Paths.get(first, more).toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* printTasks:                                                            */
    /* ---------------------------------------------------------------------- */
    private String printTasks(ReqTransfer tasks)
    {
        var buf = new StringBuilder(1024);
        buf.append("Requesting TransferTask with tag ");
        buf.append(tasks.getTag());
        buf.append(" and ");
        buf.append(tasks.getElements().size());
        buf.append(" elements:");
        for (var element : tasks.getElements()) {
            buf.append("\n  src: ");
            buf.append(element.getSourceURI());
            buf.append(", dst: ");
            buf.append(element.getDestinationURI());
            buf.append(", optional=");
            buf.append(element.getOptional());
            buf.append(", srcSharedCtx=");
            buf.append(element.getSrcSharedCtx());
            buf.append(", dstSharedCtx=");
            buf.append(element.getDestSharedCtx());
        }
        return buf.toString();
    }
}

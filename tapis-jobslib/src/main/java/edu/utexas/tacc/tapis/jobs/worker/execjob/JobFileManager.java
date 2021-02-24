package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.ApiException;
import edu.utexas.tacc.tapis.jobs.model.Job;
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
    public void createDirectories(FilesClient filesClient) throws TapisImplException
    {
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

package edu.utexas.tacc.tapis.jobs.model.submit;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.model.Job;

public final class JobSharedAppCtx 
{
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // The shared context attributes in effect for the job being processed.
    public enum JobSharedAppCtxEnum {
        SAC_EXEC_SYSTEM_ID,
        SAC_EXEC_SYSTEM_EXEC_DIR,
        SAC_EXEC_SYSTEM_INPUT_DIR,
        SAC_EXEC_SYSTEM_OUTPUT_DIR,
        SAC_ARCHIVE_SYSTEM_ID,
        SAC_ARCHIVE_SYSTEM_DIR
    }
    private static String NOT_SHARED_APP_OWNER = ""; 
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Shared application context flag assigned when app is accessed
    // and the field set is initialized only when sharing is in effect.
    private boolean                   _sharingEnabled;
    private List<JobSharedAppCtxEnum> _sharedAppCtxAttribs; // null when not sharing
    private String _sharedAppOwner="";
    
    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Called during job request processing to assign sharing attributes.
     * 
     * @param app the application to be run by the new job
     */
    public JobSharedAppCtx(TapisApp app)
    {
        // No need to do anything when not a shared application.
        
        if (StringUtils.isAllBlank(app.getSharedAppCtx())) return;

        // Assign the flag and initialize the set if we are sharing.
        _sharingEnabled = !StringUtils.isBlank(app.getSharedAppCtx());
        _sharedAppOwner = app.getSharedAppCtx();
        if (_sharingEnabled) {
            final int capacity = 6;
            _sharedAppCtxAttribs = new ArrayList<JobSharedAppCtxEnum>(capacity);
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Called by workers during job execution to conveniently access sharing attributes.
     * 
     * @param job the executing job
     */
    public JobSharedAppCtx(Job job)
    {
        // Read only access.
        _sharingEnabled = !StringUtils.isBlank(job.getSharedAppCtx());
        _sharedAppCtxAttribs = job.getSharedAppCtxAttribs();
        _sharedAppOwner=job.getSharedAppCtx();
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* calcExecSystemId:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an attribute is shared.  It must be called
     * AFTER any app value merging into the job submission request is performed. 
     * 
     * @param jobExecSystemId the non-null job request value after app merge
     * @param appExecSystemId the possibly null app definition value
     */
    public void calcExecSystemId(String jobExecSystemId, String appExecSystemId)
    {
        // Is the application shared with this user?
        if (!_sharingEnabled) return;
        
        // We only share values assigned in the app definition.
        if (StringUtils.isBlank(appExecSystemId)) return;
        
        // We share if the app and job request have the same value.
        if (appExecSystemId.equals(jobExecSystemId)) 
            _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_ID);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcArchiveSystemId:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an attribute is shared.  It must be called
     * AFTER any app value merging into the job submission request is performed. 
     * 
     * @param jobArchiveSystemId the non-null job request value after app merge
     * @param appArchiveSystemId the possibly null app definition value
     * @param jobExecSystemId the non-null execution system id
     */
    public void calcArchiveSystemId(String jobArchiveSystemId, String appArchiveSystemId,
                                    String jobExecSystemId)
    {
        // Is the application shared with this user?
        if (!_sharingEnabled) return;
        
        // If the application doesn't define an archive system, the assigned
        // system is by default the execution system.  If that is the case, 
        // we assume the application sharer intended for the default archive 
        // system to be shared.
        if (StringUtils.isBlank(appArchiveSystemId)) {
            if (jobArchiveSystemId.equals(jobExecSystemId))
                _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID);
            return;
        }
        
        // We share if the app and job request have the same value.
        if (appArchiveSystemId.equals(jobArchiveSystemId)) 
            _sharedAppCtxAttribs.add(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcExecDirSharing:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an execution system directory attribute is shared.  
     * This method must be called AFTER the app directory value has been merged into 
     * the job submission request. 
     * 
     * @param attrib the attribute to be shared
     * @param jobDir the non-null job request directory
     * @param appDir the possibly null application directory
     * @param defaultDir the default directory if not explicitly assigned
     */
    public void calcExecDirSharing(JobSharedAppCtxEnum attrib, String jobDir, 
                                   String appDir, String defaultDir)
    {
        // Is the application and exec system shared with this user?
        if (!isSharingExecSystemId()) return;
        
        // Was the directory defined in the application?
        if (StringUtils.isBlank(appDir)) {
            if (jobDir.equals(defaultDir)) _sharedAppCtxAttribs.add(attrib);
            return;
        }
        
        // Sharing's in effect if the app and job directories are the same.
        if (appDir.equals(jobDir)) _sharedAppCtxAttribs.add(attrib);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* calcArchiveDirSharing:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** This method determines whether an archive system directory attribute is shared.  
     * This method must be called AFTER the app directory value has been merged into 
     * the job submission request. 
     * 
     * @param jobDir the non-null job request directory
     * @param appDir the possibly null application directory
     * @param jobArchiveSystemId non-null archive system id 
     * @param appArchiveSystemId possibly null archive system id from app
     * @param jobExecSystemId non-null execution system id
     * @param defaultDir the default directory if not explicitly assigned
     */
    public void calcArchiveDirSharing(String jobDir, String appDir, 
                                      String jobArchiveSystemId, 
                                      String appArchiveSystemId,
                                      String jobExecSystemId,
                                      String defaultDir)
    {
        // Is the application and exec system shared with this user?
        if (!isSharingArchiveSystemId()) return;
        final var attrib = JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_DIR;
        
        // If application didn't specify an archive system and we've resolved
        // to archive on the shared execution system itself, then the archive 
        // directory is always shared.
        if (StringUtils.isBlank(appArchiveSystemId))
            if (jobArchiveSystemId.equals(jobExecSystemId) &&
                isSharingExecSystemId()) 
            {
                _sharedAppCtxAttribs.add(attrib);
                return;
            }
        
        // If the job directory was not defined in the application
        // and is assigned its default value, then it's shared.
        if (StringUtils.isBlank(appDir)) {
            if (jobDir.equals(defaultDir)) _sharedAppCtxAttribs.add(attrib);
            return;
        }
        
        // Sharing's in effect if the app and job directories are the same.
        if (appDir.equals(jobDir)) _sharedAppCtxAttribs.add(attrib);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemId:                                                       */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemId() 
    { return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_ID);}
   
    public String getSharingExecSystemAppOwner() {
    	if(isSharingExecSystemId()) {return _sharedAppOwner; }
    	else
    		return NOT_SHARED_APP_OWNER;
    }
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemExecDir:                                                  */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemExecDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_EXEC_DIR);}
    
    public String getSharingExecSystemExecDirAppOwner() {
    	if(isSharingExecSystemExecDir()) {return _sharedAppOwner; }
    	else {return NOT_SHARED_APP_OWNER;}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemInputDir:                                                 */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemInputDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_INPUT_DIR);}
    
    public String getSharingExecSystemInputDirAppOwner() {
    	if(isSharingExecSystemInputDir()) {return _sharedAppOwner;}
    	else { return NOT_SHARED_APP_OWNER; }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingExecSystemOutputDir:                                                */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingExecSystemOutputDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_EXEC_SYSTEM_OUTPUT_DIR);}
    
    public String getSharingExecSystemOutputDirAppOwner() {
    	if(isSharingExecSystemOutputDir()) { return _sharedAppOwner; }
    	else { return NOT_SHARED_APP_OWNER;}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingArchiveSystemId:                                                    */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingArchiveSystemId() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_ID);}
    
    public String getSharingArchiveSystemAppOwner() {
    	if(isSharingArchiveSystemId()) { return _sharedAppOwner;}
    	else { return NOT_SHARED_APP_OWNER;}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isSharingArchiveSystemDir:                                                   */
    /* ---------------------------------------------------------------------------- */
    public boolean isSharingArchiveSystemDir() 
    {return _sharingEnabled && _sharedAppCtxAttribs.contains(JobSharedAppCtxEnum.SAC_ARCHIVE_SYSTEM_DIR);}
    
    public String getSharingArchiveSystemDirAppOwner() {
    	if(isSharingArchiveSystemDir()) {return _sharedAppOwner;}
    	else { return NOT_SHARED_APP_OWNER;}
    }
    
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
    public boolean isSharingEnabled() {return _sharingEnabled;}
    public String getSharedAppOwner() {return _sharedAppOwner;} 
    public List<JobSharedAppCtxEnum> getSharedAppCtxResources() {return _sharedAppCtxAttribs;}
}

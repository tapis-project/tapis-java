package edu.utexas.tacc.tapis.jobs.utils;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.FilesClient.StreamedFile;
import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;

public class DataLocator {
	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DataLocator.class);
    
    // HTTP codes defined here so we don't reference jax-rs classes on the backend.
    public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private final Job _job;
   
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DataLocator(Job job) {
        this._job = job;
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getJobOutputSystemInfoForOutputListing:                                */
    /* ---------------------------------------------------------------------- */
     public JobOutputInfo getJobOutputSystemInfo(String pathName) {
    	 String systemId = "";
    	 String systemUrl = "";
    	 JobOutputInfo jobOutputInfo = null;
    	    	
    	 // archiveSytemDir and archiveSystemId always have values set
    	 if(_job.getStatus() == JobStatusType.FINISHED || 
    	    (_job.getStatus()== JobStatusType.FAILED && 
    	     _job.getRemoteOutcome() != JobRemoteOutcome.FAILED_SKIP_ARCHIVE)) 
    	 {
    		systemId = _job.getArchiveSystemId();
    		systemUrl = makeSystemUrl( _job.getArchiveSystemDir(), pathName);
    		_log.debug("Archive Path URL: " + systemUrl);
    		
    		final boolean isArchiveSystem = true;
    		jobOutputInfo =  new JobOutputInfo(systemId, systemId, systemUrl, isArchiveSystem);
    	 } else {
    		 systemId = _job.getExecSystemId();
    		 systemUrl = makeSystemUrl( _job.getExecSystemOutputDir(), pathName);
    		 final boolean isArchiveSystem = false;
     		jobOutputInfo =  new JobOutputInfo(systemId, systemId, systemUrl, isArchiveSystem);
    	 }
    	 
    	 return jobOutputInfo ;
     }
     
     /* ---------------------------------------------------------------------- */
     /* getJobOutputListings:                                                  */
     /* ---------------------------------------------------------------------- */
     public List<FileInfo> getJobOutputListings(JobOutputInfo jobOutputInfo, String tenant, 
                                                String user, int limit, int skip,
    		                                    String impersonationId, String sharedAppCtx) 
      throws TapisImplException
     {
    	 List<FileInfo> outputList = null;
    	 boolean recursiveFlag = true;
    	 
    	 // Get the File Service client 
         FilesClient filesClient = null;
		
		 filesClient = getServiceClient(FilesClient.class, user, tenant);
		        
         try {
        	outputList = filesClient.listFiles(jobOutputInfo.getSystemId(), jobOutputInfo.getSystemUrl(), 
        	                                   limit, skip, recursiveFlag, impersonationId, sharedAppCtx);
         } catch (TapisClientException e) {
            String msg = MsgUtils.getMsg("FILES_REMOTE_FILESLIST_ERROR", 
            		jobOutputInfo.getSystemId(),  jobOutputInfo.getSystemUrl(), 
            		limit, skip, _job.getOwner(), _job.getTenant(), e.getCode());
            throw new TapisImplException(msg, e, e.getCode());
         }
		
        if (outputList == null) {
        	_log.debug("Null Job output Files list returned!");
         } else {
            _log.debug("Number of Job output files returned: " + outputList.size());
         }
    	return outputList;
     }
     
     /* ---------------------------------------------------------------------- */
     /* getJobOutputDownload:                                                  */
     /* ---------------------------------------------------------------------- */
     public StreamedFile getJobOutputDownload(JobOutputInfo jobOutputInfo, 
                                              String tenant, String user, 
    		                                  boolean compress, String impersonationId, String sharedAppCtx) 
      throws TapisImplException
     {
    	 // Get the File Service client 
         FilesClient filesClient = null;
		
		 filesClient = getServiceClient(FilesClient.class, user, tenant);
		 StreamedFile streamFromFiles = null;
		 try {
			 streamFromFiles=filesClient.getFileContents(jobOutputInfo.getSystemId(), jobOutputInfo.getSystemUrl(), compress, impersonationId, sharedAppCtx);
			
		} catch (TapisClientException e) {
			String msg = MsgUtils.getMsg("FILES_REMOTE_FILESDOWNLOAD_ERROR", 
            		jobOutputInfo.getSystemId(),  jobOutputInfo.getSystemUrl(), _job.getOwner(),
            	   _job.getTenant(), e.getCode());
            throw new TapisImplException(msg, e, e.getCode());
		}
         	
        if (streamFromFiles == null) {
        	_log.debug("No files returned!");
         } else {
            _log.debug("Transferring streams using Files service");
           
         }
    	 return streamFromFiles;
     }
     
     /* ---------------------------------------------------------------------- */
     /* makeSystemUrl:                                                         */
     /* ---------------------------------------------------------------------- */
     /** Create a tapis url based on the base path on the system (input, output, exec) 
      * relative to the system's rootDir and a file pathname.  
      * The files service implicitly prefix tapis url with the system id while files listing
      *   
      * The pathName can be null or empty.
      * 
      * @param basePath the jobs base path (input, output, exec) relative to the system's rootDir
      * @param pathName the file pathname relative to the basePath
      * @return the tapis url indicating a path on the exec system.
      */
     private String makeSystemUrl(String basePath, String pathName)
     {
         String url = basePath;
         // Add the suffix.
         if (StringUtils.isBlank(pathName)) return url;
         if (url.endsWith("/") && pathName.startsWith("/")) url += pathName.substring(1);
         else if (!url.endsWith("/") && !pathName.startsWith("/")) url += "/" + pathName;
         else url += pathName;
         _log.debug("system url for path: " + url);
         return url;
     }

   
   
    /* ---------------------------------------------------------------------------- */
    /* getServiceClient:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Get a new or cached Service client.  This can only be called after
     * the request tenant and owner have be assigned.
     * 
     * @return the client
     * @throws TapisImplException
     */
    public <T> T getServiceClient(Class<T> cls,  String user, String tenant) throws TapisImplException
    {
        // Get the application client for this user@tenant.
        T client = null;
        try {
            client = ServiceClients.getInstance().getClient(
                   user, tenant, cls);
        }
        catch (Exception e) {
            var serviceName = StringUtils.removeEnd(cls.getSimpleName(), "Client");
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", serviceName, 
                                         tenant, user);
            throw new TapisImplException(msg, e,  HTTP_INTERNAL_SERVER_ERROR );
        }

        return client;
    }
}

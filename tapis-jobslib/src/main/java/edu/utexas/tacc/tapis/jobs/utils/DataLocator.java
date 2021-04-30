package edu.utexas.tacc.tapis.jobs.utils;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobIOTargets;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

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
    
    // Tapis resources.
    private TapisSystem              _executionSystem;
    private TapisSystem              _archiveSystem;
    private JobFileManager           _jobFileManager;
    private SSHConnection            _execSysConn; // always use accessor
    
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
     public JobOutputInfo getJobOutputSystemInfoForOutputListing(String pathName) {
    	 String systemId = "";
    	 String systemDir = "";
    	 String systemUrl = "";
    	 JobOutputInfo jobOutputInfo = null;
    	
    	 // archiveSytemDir and archiveSystemId always have values set
    	 if(_job.getStatus() == JobStatusType.FINISHED || _job.getStatus()== JobStatusType.FAILED && _job.getRemoteOutcome() != JobRemoteOutcome.FAILED_SKIP_ARCHIVE) {
    		systemId = _job.getArchiveSystemId();
    		systemDir = _job.getArchiveSystemDir();
    		systemUrl = makeSystemUrl(_job.getArchiveSystemId(), _job.getArchiveSystemDir(), pathName);
    		_log.debug("setting up archive path url: " + systemUrl);
    		jobOutputInfo =  new JobOutputInfo(systemId,systemId, systemUrl);
    	 } else {
    		 systemId = _job.getExecSystemId();
    		 systemDir = _job.getExecSystemOutputDir();
    		 systemUrl = makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemOutputDir(), pathName);
     		jobOutputInfo =  new JobOutputInfo(systemId,systemId, systemUrl);
    	 }
    	 
    	 return jobOutputInfo ;
     }
     
     public List<FileInfo> getJobOutputListings(JobOutputInfo jobOutputInfo, String tenant, String owner, int limit, int skip ){
    	 List<FileInfo> outputList = null;
    	
    	// Get the File Service client 
         FilesClient filesClient = null;
		try {
			filesClient = getServiceClient(FilesClient.class, owner, tenant);
		} catch (TapisImplException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         try {
        	 outputList = filesClient.listFiles(jobOutputInfo.getSystemId(), jobOutputInfo.getSystemUrl(), limit, skip, true);
		} catch (TapisClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        if (outputList == null) {
             _log.debug("Null list returned!");
         } else {
            _log.debug("Number of files returned: " + outputList.size());
             for (var f : outputList) {
                 System.out.println("\nfile:  " + f.getName());
                 System.out.println("  size:  " + f.getSize());
                 System.out.println("  time:  " + f.getLastModified());
                 System.out.println("  path:  " + f.getPath());
                 System.out.println("  type:  " + f.getType());
                 System.out.println("  owner: " + f.getOwner());
                 System.out.println("  group: " + f.getGroup());
                 System.out.println("  perms: " + f.getNativePermissions());
                 System.out.println("  mime:  " + f.getMimeType());
             }
         }
    	 return outputList;
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
         //String url = "tapis://" + systemId;
         String url = "";
         // Add the job's put input path.
        // if (basePath.startsWith("/")) url += basePath;
        //   else url += "/" + basePath;
         url = basePath;
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
    public <T> T getServiceClient(Class<T> cls,  String owner, String tenant) throws TapisImplException
    {
        // Get the application client for this user@tenant.
        T client = null;
        try {
            client = ServiceClients.getInstance().getClient(
                   owner, tenant, cls);
        }
        catch (Exception e) {
            var serviceName = StringUtils.removeEnd(cls.getSimpleName(), "Client");
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", serviceName, 
                                         tenant, owner);
            throw new TapisImplException(msg, e,  HTTP_INTERNAL_SERVER_ERROR );
        }

        return client;
    }
}

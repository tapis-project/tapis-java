package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;

public final class ApplicationTester
 extends AbsTester
{
   /* ********************************************************************** */
   /*                               Constants                                */
   /* ********************************************************************** */
   // Tracing.
   private static final Logger _log = LoggerFactory.getLogger(ApplicationTester.class);
   
   /* ********************************************************************** */
   /*                                 Fields                                 */
   /* ********************************************************************** */
   // Set to true the first time the tester parameters are validated.
   private boolean _parmsValidated = false;
   
   // Validated test parameters saved as fields for convenient access.
   private String _tenantId;
   private String _appName;
   private String _owner;
   
   /* **************************************************************************** */
   /*                                 Constructors                                 */
   /* **************************************************************************** */
   /* ---------------------------------------------------------------------------- */
   /* constructor:                                                                 */
   /* ---------------------------------------------------------------------------- */
   public ApplicationTester(JobRecovery jobRecovery)
   {
       super(jobRecovery);
   }
   
   /* ********************************************************************** */
   /*                             Public Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* canUnblock:                                                            */
   /* ---------------------------------------------------------------------- */
   @Override
   public int canUnblock(Map<String, String> testerParameters) 
    throws JobRecoveryAbortException
   {
       // Validate the tester parameters. 
       // An exception can be thrown here.
       validateTesterParameters(testerParameters);
       
       // Get the application client for this user@tenant.
       AppsClient appsClient = null;
       try {
           appsClient = ServiceClients.getInstance().
                           getClient(_owner, _tenantId, AppsClient.class);
       }
       catch (Exception e) {
           String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Apps", _tenantId, _owner);
           throw new JobRecoveryAbortException(msg, e);
       }
       if (appsClient == null) {
           String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Apps", _tenantId, _owner);
           throw new JobRecoveryAbortException(msg);
       }
       
       // Query the system for it's current availability.
       boolean available; 
       try {available = appsClient.isEnabled(_appName);}
       catch (Exception e) {
               String msg = MsgUtils.getMsg("JOBS_RECOVERY_DB_ACCESS", _jobRecovery.getId(), 
                                            "SystemDao", e.getMessage());
               throw new JobRecoveryAbortException(msg, e);
         }
       
       // Return whether is the system is marked available.
       if (available) return DEFAULT_RESUBMIT_BATCHSIZE;
       else return NO_RESUBMIT_BATCHSIZE;
   }
   
   /* ********************************************************************** */
   /*                            Private Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* validateTesterParameters:                                              */
   /* ---------------------------------------------------------------------- */
   /** Validate only the parameters required to run the test. If validation 
    * fails on the first attempt, then the exception thrown will cause the
    * recovery abort and all its jobs to be failed. If validation success
    * on the first attempt, then we skip validation in this object from now on.
    * 
    * @param testerParameters the test parameter map
    * @throws JobRecoveryAbortException on validation error
    */
   private void validateTesterParameters(Map<String, String> testerParameters)
    throws JobRecoveryAbortException
   {
       // No need to validate more than once.
       if (_parmsValidated) return;

       // We need the app owner to be specified.
       _owner = testerParameters.get("owner");
       if (StringUtils.isBlank(_owner)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "owner");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }
       
       // We need the tenantId to be specified.
       _tenantId = testerParameters.get("tenantId");
       if (StringUtils.isBlank(_tenantId)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "tenantId");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }

       // We need the app name to be specified.
       _appName = testerParameters.get("name");
       if (StringUtils.isBlank(_appName)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "name");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }
       
       // We're good if we get here.
       _parmsValidated = true;
   }
}

package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.api.GeneralApi;
import edu.utexas.tacc.tapis.files.client.gen.model.RespBasic;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;

public final class ServiceConnectionTester
extends AbsTester
{
   /* ********************************************************************** */
   /*                               Constants                                */
   /* ********************************************************************** */
   // Tracing.
   private static final Logger _log = LoggerFactory.getLogger(ServiceConnectionTester.class);
   
   // Source identity.
   private static final String USER   = "jobs-recovery";
   private static final String TENANT = "none";
   
   // Health check success state.
   private static final String SUCCESS = "success";
   
   /* ********************************************************************** */
   /*                                 Fields                                 */
   /* ********************************************************************** */
   // Set to true the first time the tester parameters are validated.
   private boolean _parmsValidated = false;
   
   // Test parameters.
   private String _baseUrl;
   private String _serviceName;
   
   /* ********************************************************************** */
   /*                               Constructors                             */
   /* ********************************************************************** */
   /* -----------------------------------------------------------------------*/
   /* constructor:                                                           */
   /* -----------------------------------------------------------------------*/
   public ServiceConnectionTester(JobRecovery jobRecovery)
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
       
       // Get the client class
       Object client;
       try {client = ServiceClients.getInstance().getClient(USER, TENANT, _serviceName);}
       catch (Exception e) {
           String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", _serviceName, TENANT, USER);
           throw new JobRecoveryAbortException(msg, e);
       }  
       if (client == null) {
           String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", _serviceName, TENANT, USER);
           throw new JobRecoveryAbortException(msg);
       }
       
       // See if the service is healthy.
       boolean healthy = isHealthy(client);
       
       // Return whether is the system is marked available.
       if (healthy) return DEFAULT_RESUBMIT_BATCHSIZE;
       else return NO_RESUBMIT_BATCHSIZE;
   }

   /* ********************************************************************** */
   /*                            Private Methods                             */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* isHealthy:                                                             */
   /* ---------------------------------------------------------------------- */
   private boolean isHealthy(Object client) throws JobRecoveryAbortException
   {
       // Call each service's healthcheck.
       boolean healthy = switch (_serviceName) {
           case TapisConstants.SERVICE_NAME_FILES   -> getFilesHealth((FilesClient)client);
           case TapisConstants.SERVICE_NAME_APPS    -> getAppsHealth((AppsClient)client);
           case TapisConstants.SERVICE_NAME_SYSTEMS -> getSystemsHealth((SystemsClient)client);
           default -> {
               String msg = MsgUtils.getMsg("TAPIS_UNKNOWN_CLIENT_CLASS", _serviceName);
               throw new JobRecoveryAbortException(msg);
           }
       };
       
       return healthy;
   }
   
   /* ---------------------------------------------------------------------- */
   /* getFilesHealth:                                                        */
   /* ---------------------------------------------------------------------- */
   private boolean getFilesHealth(FilesClient client) 
   {
       boolean healthy = false;
       try {
           GeneralApi api = client.getGeneralApi();
           RespBasic resp = api.healthCheck();
           if (SUCCESS.equals(resp.getStatus())) healthy = true;
       } catch (Exception e) {}
       return healthy;
   }
   
   /* ---------------------------------------------------------------------- */
   /* getAppsHealth:                                                         */
   /* ---------------------------------------------------------------------- */
   private boolean getAppsHealth(AppsClient client) 
   {
       boolean healthy = false;
       try {
           String check = client.checkHealth();
           if (SUCCESS.equals(check)) healthy = true;
       } catch (Exception e) {}
       return healthy;
   }
   
   /* ---------------------------------------------------------------------- */
   /* getSystemsHealth:                                                      */
   /* ---------------------------------------------------------------------- */
   private boolean getSystemsHealth(SystemsClient client) 
   {
       boolean healthy = false;
       try {
           String check = client.checkHealth();
           if (SUCCESS.equals(check)) healthy = true;
       } catch (Exception e) {}
       return healthy;
   }
   
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

       // Check each required parameter.
       _baseUrl = testerParameters.get("baseUrl");
       if (StringUtils.isBlank(_baseUrl)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "baseUrl");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }
       
       _serviceName = testerParameters.get("serviceName");
       if (StringUtils.isBlank(_serviceName)) {
           String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateTesterParameters", 
                                        "serviceName");
           _log.error(msg);
           throw new JobRecoveryAbortException(msg);
       }
       
       // We're good if we get here.
       _parmsValidated = true;
   }
}

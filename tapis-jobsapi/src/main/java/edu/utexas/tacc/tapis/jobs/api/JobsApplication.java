package edu.utexas.tacc.tapis.jobs.api;

import java.util.ArrayList;
import java.util.Map;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import io.swagger.v3.jaxrs2.integration.resources.AcceptHeaderOpenApiResource;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeIn;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;

@OpenAPIDefinition(
        info = @Info(title = "Tapis Jobs API",
                     version = "0.1",
                     description = "The Tapis Jobs API executes jobs on Tapis systems.",
                     license = @License(name = "3-Clause BSD License", url = "https://opensource.org/licenses/BSD-3-Clause"),
                     contact = @Contact(name = "CICSupport", 
                                        email = "cicsupport@tacc.utexas.edu")),
        tags = {@Tag(name = "jobs", description = "manage job execution and data"),
                @Tag(name = "general", description = "informational endpoints")},
        servers = {@Server(url = "http://localhost:8080/v3", description = "Local test environment")},
        externalDocs = @ExternalDocumentation(description = "Tapis Home",
                                     url = "https://tacc-cloud.readthedocs.io/projects/agave/en/latest/")
)
@SecurityScheme(
        name="TapisJWT",
        description="Tapis signed JWT token authentication",
        type=SecuritySchemeType.APIKEY,
        in=SecuritySchemeIn.HEADER,
        paramName="X-Tapis-Token"
)
// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("/jobs")
public class JobsApplication 
extends ResourceConfig
{
   // The table we query to test database connectivity.
   private static final String QUERY_TABLE = "jobs";
   
   public JobsApplication()
   {
       // ------------------ Unrecoverable Errors ------------------
       // Log our existence.
       System.out.println("**** Starting tapis-jobsapi ****");
       
       // Register the swagger resources that allow the 
       // documentation endpoints to be automatically generated.
       register(OpenApiResource.class);
       register(AcceptHeaderOpenApiResource.class);
       
       // We specify what packages JAX-RS should recursively scan
       // to find annotations.  By setting the value to the top-level
       // tapis directory in all projects, we can use JAX-RS annotations
       // in any tapis class.  In particular, the filter classes in 
       // tapis-sharedapi will be discovered whenever that project is
       // included as a maven dependency.
       packages("edu.utexas.tacc.tapis");
       setApplicationName(TapisConstants.SERVICE_NAME_JOBS); 
       
       // Initialize our parameters.  A failure here is unrecoverable.
       RuntimeParameters parms = null;
       try {parms = RuntimeParameters.getInstance();}
           catch (Exception e) {
               // We don't depend on the logging subsystem.
               System.out.println("**** FAILURE TO INITIALIZE: tapis-jobsapi RuntimeParameters [ABORTING] ****");
               e.printStackTrace();
               System.exit(1);
           }
       System.out.println("**** SUCCESS:  RuntimeParameters read ****");
       
       // Initialize local error list.
       var errors = new ArrayList<String>(); // cumulative error count
       
       // Enable more detailed SSH logging if the node name is not null.
       SSHConnection.setLocalNodeName(parms.getLocalNodeName());
       
       // ---------------- Initialize Security Filter --------------
       // Required to process any requests.
       JWTValidateRequestFilter.setService(TapisConstants.SERVICE_NAME_JOBS);
       JWTValidateRequestFilter.setSiteId(parms.getSiteId());
       
       // ------------------- Recoverable Errors -------------------
       // ----- Tenant Map Initialization
       // Force runtime initialization of the tenant manager.  This creates the
       // singleton instance of the TenantManager that can then be accessed by
       // all subsequent application code--including filters--without reference
       // to the tenant service base url parameter.
       Map<String,Tenant> tenantMap = null;
       try {
           // The base url of the tenants service is a required input parameter.
           // We actually retrieve the tenant list from the tenant service now
           // to fail fast if we can't access the list.
           String url = parms.getTenantBaseUrl();
           tenantMap = TenantManager.getInstance(url).getTenants();
       } catch (Exception e) {
           // We don't depend on the logging subsystem.
           errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi TenantManager ****\n" + e.getMessage());
           e.printStackTrace();
       }
       if (tenantMap != null) {
           System.out.println("**** SUCCESS:  " + tenantMap.size() + " tenants retrieved ****");
           String s = "Tenants:\n";
           for (String tenant : tenantMap.keySet()) s += "  " + tenant + "\n";
           System.out.println(s);
       } else 
    	   System.out.println("**** FAILURE TO INITIALIZE: tapis-jobsapi TenantManager - No Tenants ****");
       
       // ----- Service JWT Initialization
       ServiceContext serviceCxt = ServiceContext.getInstance();
       try {
                serviceCxt.initServiceJWT(parms.getSiteId(), TapisConstants.SERVICE_NAME_JOBS, 
    	    	                          parms.getServicePassword());
    	}
       	catch (Exception e) {
            errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi ServiceContext ****\n" + e.getMessage());
            e.printStackTrace();
       	}
       if (serviceCxt.getServiceJWT() != null) {
    	   var targetSites = serviceCxt.getServiceJWT().getTargetSites();
    	   int targetSiteCnt = targetSites != null ? targetSites.size() : 0;
    	   System.out.println("**** SUCCESS:  " + targetSiteCnt + " target sites retrieved ****");
    	   if (targetSites != null) {
    		   String s = "Target sites:\n";
    		   for (String site : targetSites) s += "  " + site + "\n";
    		   System.out.println(s);
    	   }
       }
       
       // ----- Database Initialization
       try {JobsImpl.getInstance().ensureDefaultQueueIsDefined();}
	    catch (Exception e) {
            errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi Database ****\n" + e.getMessage());
	    	e.printStackTrace();
	    }
       
       // ------ Queue Initialization 
       // By getting the singleton instance of the queue manager
       // we also cause all job queues and exchanges to be initialized.
       // There is some redundancy here since each front-end and
       // each worker initializes all queue artifacts.  Not a problem, 
       // but there's room for improvement.
       try {initializeJobQueueManager();} // called for side effect
        catch (Exception e) {
            errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi JobQueueManager ****\n" + e.getMessage());
            e.printStackTrace();
        }
        
       // We're done.
       System.out.println("\n**********************************************");
       System.out.println("**** tapis-jobsapi Initialized [errors=" + errors.size() + "] ****");
       System.out.println("**********************************************\n");
       
       // This is an effective but somewhat crude way to abort.
       if (!errors.isEmpty()) {
           System.out.println("\n");
           for (var s : errors) System.out.println(s);
           System.exit(1);
       }
   }
   
   /** Initialize rabbitmq vhost and our standard queues and exchanges.  VHost initialization
    * requires the overall administrator's credentials to create the vhost and its user if
    * they don't already exist.
    */
   private void initializeJobQueueManager()
   {
       // This can throw a runtime exception.
       JobQueueManager.getInstance(JobQueueManager.initParmsFromRuntime());
   }
}

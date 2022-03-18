package edu.utexas.tacc.tapis.security.api;

import java.util.ArrayList;
import java.util.Map;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;

import edu.utexas.tacc.tapis.security.api.utils.TenantInit;
import edu.utexas.tacc.tapis.security.authz.impl.RoleImpl;
import edu.utexas.tacc.tapis.security.config.RuntimeParameters;
import edu.utexas.tacc.tapis.security.secrets.VaultManager;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
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

/** Main JAX-RS application configuration class. This is the first SK code
 * that executes when the web application is loaded. 
 * 
 * @author rcardone
 */
@OpenAPIDefinition(
        info = @Info(title = "Tapis Security API",
                     version = "0.1",
                     description = "The Tapis Security API provides access to the " +
                     "Tapis Security Kernel authorization and secrets facilities.",
                     license = @License(name = "3-Clause BSD License", url = "https://opensource.org/licenses/BSD-3-Clause"),
                     contact = @Contact(name = "CICSupport", 
                                        email = "cicsupport@tacc.utexas.edu")),
        tags = {@Tag(name = "role", description = "manage roles and permissions"),
                @Tag(name = "user", description = "assign roles and permissions to users"),
                @Tag(name = "share", description = "share resources among users"),
                @Tag(name = "vault", description = "manage application and user secrets"),
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
@ApplicationPath("/security")
public class SecurityApplication 
 extends ResourceConfig
{
    public SecurityApplication()
    {
        // ------------------ Unrecoverable Errors ------------------
        // Log our existence.
        System.out.println("**** Starting tapis-securityapi ****");
        
        // Register the swagger resources that allow the 
        // documentation endpoints to be automatically generated.
        register(OpenApiResource.class);
        register(AcceptHeaderOpenApiResource.class);
        
        // We specify what packages JAX-RS should recursively scan
        // to find annotations.  By setting the value to the top-level
        // aloe directory in all projects, we can use JAX-RS annotations
        // in any aloe class.  In particular, the filter classes in 
        // tapis-sharedapi will be discovered whenever that project is
        // included as a maven dependency.
        packages("edu.utexas.tacc.tapis");
        setApplicationName("security"); 
        
        // Initialize our parameters.  A failure here is unrecoverable.
        RuntimeParameters parms = null;
        try {parms = RuntimeParameters.getInstance();}
            catch (Exception e) {
                // We don't depend on the logging subsystem.
                System.out.println("**** FAILURE TO INITIALIZE: tapis-securityapi RuntimeParameters [ABORTING] ****");
                e.printStackTrace();
                System.exit(1);
            }
        System.out.println("**** SUCCESS:  RuntimeParameters read ****");
        
        // Initialize local error list.
        var errors = new ArrayList<String>(); // cumulative error count
        
        // ---------------- Initialize Security Filter --------------
        // Required to process any requests.
        JWTValidateRequestFilter.setService(TapisConstants.SERVICE_NAME_SECURITY);
        JWTValidateRequestFilter.setSiteId(parms.getSiteId());
        
        // ------------------- Recoverable Errors -------------------
        // ------- Vault Initialization
        // Force runtime initialization of vault.
        boolean success = false;
        try {VaultManager.getInstance(parms); success = true;}
            catch (Exception e) {
                // We don't depend on the logging subsystem.
                errors.add("**** FAILURE TO INITIALIZE: tapis-securityapi VaultManager ****\n" + e.getMessage());
                e.printStackTrace();
            }
        if (success) System.out.println("**** SUCCESS:  VaultManager initialized ****");
        
        // ------- Database Initialization
        success = false;
        try {RoleImpl.getInstance().queryDB("sk_role"); success = true;}
         catch (Exception e) {
             errors.add("**** FAILURE TO INITIALIZE: tapis-securitysapi Database ****\n" + e.getMessage());
             e.printStackTrace();
         }
        if (success) System.out.println("**** SUCCESS:  PostgreSQL Database initialized ****");
        
        // ------- Tenants Initialization
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
            errors.add("**** FAILURE TO INITIALIZE: tapis-securityapi TenantManager ****\n" + e.getMessage());
            e.printStackTrace();
        }
        if (tenantMap != null) {
            System.out.println("**** SUCCESS:  " + tenantMap.size() + " tenants retrieved ****");
            String s = "Tenants:\n";
            for (String tenant : tenantMap.keySet()) s += "  " + tenant + "\n";
            System.out.println(s);
        } else
        	System.out.println("**** FAILURE TO INITIALIZE: tapis-securityapi TenantManager - No Tenants ****");
        
        // ------- Authorization Initialization
        // Initialize tenant roles and administrators.
        success = false;
        try {TenantInit.initializeTenants(tenantMap); success = true;}
            catch (Exception e) {
                // We don't depend on the logging subsystem.
                errors.add("**** FAILURE TO INITIALIZE: tapis-securityapi TenantInit ****\n" + e.getMessage());
                e.printStackTrace();
            }
        if (success) System.out.println("**** SUCCESS:  Tenant admins initialized ****");
        
        // We're done.
        System.out.println("\n**************************************************");
        System.out.println("**** tapis-securityapi Initialized [errors=" + errors.size() + "] ****");
        System.out.println("**************************************************\n");

        // This is an effective but somewhat crude way to abort.
        if (!errors.isEmpty()) {
            System.out.println("\n");
            for (var s : errors) System.out.println(s);
            System.exit(1);
        }
}
}

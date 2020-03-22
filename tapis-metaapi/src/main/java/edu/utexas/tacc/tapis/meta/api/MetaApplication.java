package edu.utexas.tacc.tapis.meta.api;

import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import io.swagger.v3.jaxrs2.integration.resources.AcceptHeaderOpenApiResource;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;


@ApplicationPath("/v3/meta")
public class MetaApplication extends ResourceConfig {
  
  public MetaApplication()
  {
    // Log our existence.
    System.out.println("**** Starting tapis-metaapi ****");
    
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
    setApplicationName("meta");
  
    // Force runtime initialization of the tenant manager.  This creates the
    // singleton instance of the TenantManager that can then be accessed by
    // all subsequent application code--including filters--without reference
    // to the tenant service base url parameter.
    try {
      // The base url of the tenants service is a required input parameter.
      // We actually retrieve the tenant list from the tenant service now
      // to fail fast if we can't access the list.
      String url = RuntimeParameters.getInstance().getTenantBaseUrl();
      TenantManager.getInstance(url).getTenants();
    } catch (Exception e) {
      // We don't depend on the logging subsystem.
      System.out.println("**** FAILURE TO INITIALIZE: tapis-metaapi ****");
      e.printStackTrace();
      throw e;
    }
  
  
  }

}

package edu.utexas.tacc.tapis.systems.api;

import javax.ws.rs.ApplicationPath;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import io.swagger.v3.jaxrs2.integration.resources.AcceptHeaderOpenApiResource;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;

import java.net.URI;

// The path here is appended to the context root and is configured to work when invoked in a standalone
// container (command line) and in an IDE (eclipse).
// NOTE: When running using tomcat this path should match the war file name (v3#systems.war) for running
//       in IntelliJ IDE as well as from a docker container.
// NOTE: When running using tomcat in IntelliJ IDE the live openapi docs contain /v3/systems in the URL
//       but when running from a docker container they do not.
// NOTE: When running using grizzly in IntelliJ IDE or from docker container the live openapi docs do not
//       contain /v3/systems in the URL.
// NOTE: When running from IntelliJ IDE the live openapi docs do not contain the top level paths
//       GET /v3/systems, POST /v3/systems, GET /v3/systems/{sysName} and POST /v3/systems/{sysName}
//       but the file on disk (tapis-systemsapi/src/main/resources/openapi.json) does contains the paths.
// NOTE: All the paths in the openapi file on disk (tapis-systemsapi/src/main/resources/openapi.json) are
//       missing the prefix /v3/systems
@ApplicationPath("v3/systems")
public class SystemsApplication extends ResourceConfig
{
  // For all logging use println or similar so we do not have a dependency on a logging subsystem.
  public SystemsApplication()
  {
    // Log our existence.
    System.out.println("**** Starting tapis-systems ****");

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

    // Set the application name.
    // Note that this has no impact on base URL
    setApplicationName("systems");

    // Perform remaining init steps in try block so we can print a fatal error message if something goes wrong.
    try {

      // Initialize tenant manager singleton. This can be used by all subsequent application code, including filters.
      // The base url of the tenants service is a required input parameter.
      // Retrieve the tenant list from the tenant service now to fail fast if we can't access the list.
      String url = RuntimeParameters.getInstance().getTenantsSvcURL();
      TenantManager.getInstance(url).getTenants();

      // TODO Initialize ServiceJWT for injection into SystemsServiceImpl

      // Initialize bindings for HK2 dependency injection
      register(new AbstractBinder() {
        @Override
        protected void configure() {
          bind(SystemsServiceImpl.class).to(SystemsService.class);
          bind(SystemsDaoImpl.class).to(SystemsDao.class);
          bind(ServiceJWT.class).to(ServiceJWT.class);
          bind(SKClient.class).to(SKClient.class);
        }
      });

    } catch (Exception e) {
      // This is a fatal error
      System.out.println("**** FAILURE TO INITIALIZE: tapis-systemsapi ****");
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Embedded Grizzly HTTP server
   */
  public static void main(String[] args) throws Exception
  {
    final URI BASE_URI = URI.create("http://0.0.0.0:8080/v3/systems");
    ResourceConfig config = new SystemsApplication();
    final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
    server.start();
  }
}

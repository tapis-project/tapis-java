package edu.utexas.tacc.tapis.systems.api;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;

import io.swagger.v3.jaxrs2.integration.resources.AcceptHeaderOpenApiResource;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;

// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse).
// NOTE: This path should match the war file name (v3#systems.war) for running
//       in IntelliJ IDE as well as from a docker container.
// NOTE: When running from IntelliJ IDE the live openapi docs contain /v3/systems in the URL
//       but when running from a docker container they do not.
// NOTE: When running from IntelliJ IDE the live openapi docs do not contain the top level paths
//       GET /v3/systems, POST /v3/systems, GET /v3/systems/{sysName} and POST /v3/systems/{sysName}
//       but the file on disk (tapis-systemsapi/src/main/resources/openapi.json) does contains the paths.
// NOTE: All the paths in the openapi file on disk (tapis-systemsapi/src/main/resources/openapi.json) are
//       missing the prefix /v3/systems
@ApplicationPath("v3/systems")
public class SystemsApplication extends ResourceConfig
{
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

    // Finally set the application name
    // This appears to have no impact on base URL
    setApplicationName("systems");
  }
}

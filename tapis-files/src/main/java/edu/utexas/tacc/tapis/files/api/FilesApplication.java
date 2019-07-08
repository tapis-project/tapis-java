package edu.utexas.tacc.tapis.files.api;

import javax.ws.rs.ApplicationPath;

import edu.utexas.tacc.tapis.files.api.resources.ListingsResource;
import edu.utexas.tacc.tapis.files.api.resources.SystemsResource;
import edu.utexas.tacc.tapis.files.lib.dao.StorageSystemsDAO;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;

// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("files")
public class FilesApplication extends ResourceConfig
{
	public FilesApplication()
	{
	    // We specify what packages JAX-RS should recursively scan
        // to find annotations.  By setting the value to the top-level
        // aloe directory in all projects, we can use JAX-RS annotations
        // in any aloe class.  In particular, the filter classes in 
        // tapis-sharedapi will be discovered whenever that project is
        // included as a maven dependency.
		register(SystemsResource.class);
		register(ListingsResource.class);

		register(JacksonFeature.class);
		register(JWTValidateRequestFilter.class);

		OpenApiResource openApiResource = new OpenApiResource();
		register(openApiResource);
		setApplicationName("files");


		// Needed for dependency injection to work in the Resource classes.
		register(new AbstractBinder() {
			@Override
			public void configure() {
				bind(StorageSystemsDAO.class).to(StorageSystemsDAO.class);
			}
		});

	}
}

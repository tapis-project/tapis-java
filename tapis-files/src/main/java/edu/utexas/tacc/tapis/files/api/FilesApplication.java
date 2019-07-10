package edu.utexas.tacc.tapis.files.api;

import javax.ws.rs.ApplicationPath;

import edu.utexas.tacc.tapis.files.api.resources.ListingsResource;
import edu.utexas.tacc.tapis.files.api.resources.SystemsResource;
import edu.utexas.tacc.tapis.files.lib.dao.StorageSystemsDAO;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;



import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;

import java.net.URI;

// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("files")
public class FilesApplication extends ResourceConfig
{
	public FilesApplication()
	{
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

	public static void main(String[] args) throws Exception {
		final URI BASE_URI = URI.create("http://0.0.0.0:8080/files");
		ResourceConfig config = new FilesApplication();
		System.out.println(config.getResources());
		final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
		server.start();
	}
}

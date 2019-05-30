package edu.utexas.tacc.tapis.files.api;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;

// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("/")
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
		packages("edu.utexas.tacc.tapis.files");
		setApplicationName("files");
	}
}

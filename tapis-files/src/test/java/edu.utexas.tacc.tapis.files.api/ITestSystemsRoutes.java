package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.resources.SystemsResource;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.glassfish.jersey.test.JerseyTestNg;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.nio.file.Files;


@Test(groups={"integration"})
public class ITestSystemsRoutes extends JerseyTestNg.ContainerPerClassTest{

    @Override
    protected Application configure() {
        Application conf =  new ResourceConfig(SystemsResource.class);
        return conf;
    }

    @Test
    public void testSystemsList() {
        Assert.assertEquals(1, 1);
    }

    @Test
    public void testSystemsListing(){
        Response response = target().path("systems")
                .request()
                .get(Response.class);

        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("{\"data\":{}}", response.readEntity(String.class));
    }
}

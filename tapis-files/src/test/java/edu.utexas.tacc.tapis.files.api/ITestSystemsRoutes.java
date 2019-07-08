package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.resources.SystemsResource;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.glassfish.jersey.test.JerseyTestNg;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.nio.file.Files;


@Test(groups={"integration"})
public class ITestSystemsRoutes extends JerseyTestNg.ContainerPerClassTest{

    //TODO: move that into a resource
    private final String jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJ3c28yLm9yZy9wcm9kdWN0cy9hbSIsImV4cCI6MjM4NDQ4MTcxMzg0MiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy9zdWJzY3JpYmVyIjoiam1laXJpbmciLCJodHRwOi8vd3NvMi5vcmcvY2xhaW1zL2FwcGxpY2F0aW9uaWQiOiI0NCIsImh0dHA6Ly93c28yLm9yZy9jbGFpbXMvYXBwbGljYXRpb25uYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy9hcHBsaWNhdGlvbnRpZXIiOiJVbmxpbWl0ZWQiLCJodHRwOi8vd3NvMi5vcmcvY2xhaW1zL2FwaWNvbnRleHQiOiIvZ2VvYXBpIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy92ZXJzaW9uIjoiMi4wIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy90aWVyIjoiVW5saW1pdGVkIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy9rZXl0eXBlIjoiUFJPRFVDVElPTiIsImh0dHA6Ly93c28yLm9yZy9jbGFpbXMvdXNlcnR5cGUiOiJBUFBMSUNBVElPTl9VU0VSIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy9lbmR1c2VyIjoiam1laXJpbmciLCJodHRwOi8vd3NvMi5vcmcvY2xhaW1zL2VuZHVzZXJUZW5hbnRJZCI6IjEwIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy9lbWFpbGFkZHJlc3MiOiJ0ZXN0dXNlcjNAdGVzdC5jb20iLCJodHRwOi8vd3NvMi5vcmcvY2xhaW1zL2Z1bGxuYW1lIjoiRGV2IFVzZXIiLCJodHRwOi8vd3NvMi5vcmcvY2xhaW1zL2dpdmVubmFtZSI6IkRldiIsImh0dHA6Ly93c28yLm9yZy9jbGFpbXMvbGFzdG5hbWUiOiJVc2VyIiwiaHR0cDovL3dzbzIub3JnL2NsYWltcy9wcmltYXJ5Q2hhbGxlbmdlUXVlc3Rpb24iOiJOL0EiLCJodHRwOi8vd3NvMi5vcmcvY2xhaW1zL3JvbGUiOiJJbnRlcm5hbC9ldmVyeW9uZSIsImh0dHA6Ly93c28yLm9yZy9jbGFpbXMvdGl0bGUiOiJOL0EifQ.XPtNwdjNS8HKHTcjtSIm2HGHC-yvsyHYt0vG2F9ZFftnO8Fhf4-Gp-iWdHibyeK9wC6ZZVJcTOub9X1N-5TDQqjaZCN7PaiERbOX6aRQjIiHAL8pU6_7HtRQzFQqgD5B59GGp8NeGvC6rJI539nw9a5bemVK_cVGA7Aokvhn7ZOiPFqfvZ6sM46IL9frm6PikuYXlFbep7FFWvGn7Au7eVddBpHdtD79MpHx7cyLfqgWJtR6SrSvTgqAC6498xMRmpkuQz5Xutm4rGa4_vunR0d53upiMX9jpwrKlCyZ96G93br-t-q7hwNYzp-KM0mvfqcQVQ6B7MSbRyWOqragjQ";

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        return new FilesApplication();
    }

    @Test
    public void testSystemsList() {
        Assert.assertEquals(1, 1);
    }

    @Test
    public void testSystemsListing(){
        Response response = target().path("/systems")
                .request()
                .header("x-jwt", this.jwt)
                .get(Response.class);
        Assert.assertEquals(200, response.getStatus());
    }
}

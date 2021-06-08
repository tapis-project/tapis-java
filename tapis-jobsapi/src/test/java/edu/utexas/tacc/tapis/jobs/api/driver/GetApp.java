package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.util.Properties;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;

public class GetApp 
{
    /** Driver
     * 
     * @param args should contain the name of a file in the ./requests directory
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var getApp = new GetApp();
        getApp.get(args);
    }
    
    /** Create a tapis application from input in a file.
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void get(String[] args) throws IOException, TapisClientException
    {
        // Check the input.
        if (args.length < 3) {
            System.out.println("Supply the name, the version of an application and the profile name in that order.");
            System.out.println("Generated profile path: " + TestUtils.getProfilePathTemplate());
            return;
        }
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[2]);
        
        // Create the app.
        System.out.println("Retrieving app " + args[0] + " version " + args[1] + ".");
        var appsClient = new AppsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        var app = appsClient.getApp(args[0], args[1]);
        System.out.println(app.toString());
    }
}

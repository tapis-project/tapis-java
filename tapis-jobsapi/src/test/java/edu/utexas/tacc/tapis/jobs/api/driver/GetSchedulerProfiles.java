package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;

public class GetSchedulerProfiles 
{
    /** Driver
     * 
     * @param args should contain the name of a file in the ./requests directory
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var getProfiles = new GetSchedulerProfiles();
        getProfiles.get(args);
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
        if (args.length < 1) {
            System.out.println("Supply the profile name.");
            System.out.println("Generated profile path: " + TestUtils.getProfilePathTemplate());
            return;
        }
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[0]);
        
        // Create the app.
        var sysClient = new SystemsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        var profiles = sysClient.getSchedulerProfiles();
        System.out.println("Number of profiles: " + profiles.size());
        for (int i = 0; i < profiles.size(); i++) {
            System.out.println(profiles.get(i));
        }
    }
}
